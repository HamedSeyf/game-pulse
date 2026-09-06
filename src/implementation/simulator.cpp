module;

#include <spdlog/spdlog.h>

module game_pulse.simulator;

import game_pulse.pipeline;
import game_pulse.queue;

import <cassert>;
import <optional>;
import <utility>;


Simulator::Simulator(
    std::shared_ptr<TickClock> tickClock,
    std::shared_ptr<Queue> queue,
    T_ID playerID,
    const std::span<T_ID> otherPlayerIDs,
    SimulatorTypes::TEventGenerationWeights eventGenerationWeights,
    std::uint64_t randomSeed
)
    :
    _playerID(playerID),
    _otherPlayerIDs(otherPlayerIDs.begin(), otherPlayerIDs.end()),
    _tickClock(std::move(tickClock)),
    _queue(queue),
    _eventGenerationCutoffs(BuildEventGenerationCutoffs(eventGenerationWeights)),
    _randomEngine(randomSeed),
    _targetDistribution{ 0, otherPlayerIDs.size() - 1 },
    _damageDistribution{ 1, PlayerMaxHealth }
{
    if (!_tickClock || !queue || otherPlayerIDs.empty())
    {
        throw std::invalid_argument{ "Invalid tickClock, queue or otherPlayerIDs passed to Simulator's ctor." };
    }
}

Simulator::TEventGenerationCutoffs Simulator::BuildEventGenerationCutoffs(const SimulatorTypes::TEventGenerationWeights& weights)
{
    const auto isValidWeight = [](const double weight) noexcept
        {
            return std::isfinite(weight) && weight >= 0.0;
        };

    if (!isValidWeight(weights.spawnWeight) ||
        !isValidWeight(weights.moveWeight) ||
        !isValidWeight(weights.shotWeight) ||
        !isValidWeight(weights.noEventWeight))
    {
        throw std::invalid_argument{"Event-generation weights must be finite and nonnegative."};
    }

    const double totalWeight =
        weights.spawnWeight +
        weights.moveWeight +
        weights.shotWeight +
        weights.noEventWeight;

    if (!std::isfinite(totalWeight) || totalWeight <= 0.0)
    {
        throw std::invalid_argument{"Event-generation weights must have a positive finite total."};
    }

    const double inverseTotal = 1.0 / totalWeight;

    const double spawnEnd = weights.spawnWeight * inverseTotal;

    const double moveEnd = spawnEnd + weights.moveWeight * inverseTotal;

    const double shotEnd = (weights.noEventWeight == 0.0 ? 1.0 : moveEnd + weights.shotWeight * inverseTotal);

    return TEventGenerationCutoffs{
        .spawnEnd = spawnEnd,
        .moveEnd = moveEnd,
        .shotEnd = shotEnd,
    };
}

std::optional<EventTypes::Event> Simulator::CreateRandomEvent(const TickClock::Tick tick)
{
    const double sample = _unitDistribution(_randomEngine);

    const auto makeRandomPosition = [this]() -> TPlayerPositionType
        {
            return {
                _unitDistribution(_randomEngine),
                _unitDistribution(_randomEngine)
            };
        };

    if (sample < _eventGenerationCutoffs.spawnEnd)
    {
        return EventTypes::Event
        {
            .id = GlobalID::NextID(),
            .tick = tick,
            .type = EventTypes::EventType::Spawn,
            .spawn = EventTypes::SpawnEvent
            {
                .playerId = _playerID,
                .position = makeRandomPosition()
            }
        };
    }
    else if (sample < _eventGenerationCutoffs.moveEnd)
    {
        return EventTypes::Event
        {
            .id = GlobalID::NextID(),
            .tick = tick,
            .type = EventTypes::EventType::Move,
            .move = EventTypes::MoveEvent
            {
                .playerId = _playerID,
                .position = makeRandomPosition()
            }
        };
    }
    else if (sample < _eventGenerationCutoffs.shotEnd)
    {
        return EventTypes::Event
        {
            .id = GlobalID::NextID(),
            .tick = tick,
            .type = EventTypes::EventType::Shot,
            .shot = EventTypes::ShotEvent
            {
                .shooterId = _playerID,
                .targetId = _otherPlayerIDs[_targetDistribution(_randomEngine)],
                .damage = _damageDistribution(_randomEngine)
            }
        };
    }

    return std::nullopt;
}

bool Simulator::OnStateTransitionLocked(const SimulatorTypes::TSimulatorStateMachineState newState) noexcept
{
    if (!TStateMachine::OnStateTransitionLocked(newState))
    {
        return false;
    }

    try
    {
        if (newState == SimulatorTypes::TSimulatorStateMachineState::InProgress)
        {
            auto queue = _queue.lock();
            if (!queue)
            {
                spdlog::error("Simulator failed to acquire queue on start. Rolling back state transition. PlayerID ID: {}", _playerID);
                return false;
            }

            if (const auto result = queue->RegisterSimulator(_playerID); result)
            {
                _queueRegistrationHandle = result.value();
            }
            else
            {
                spdlog::error("Simulator failed to register with queue. Rolling back state transition. PlayerID: {}", _playerID);
                return false;
            }

            _workerThread = std::jthread([this](std::stop_token stopToken)
                {
                    WorkerMain(stopToken);
                }
            );
        }
        else if (newState == SimulatorTypes::TSimulatorStateMachineState::Stopped)
        {
            _workerThread.request_stop();
        }
    }
    catch (const std::exception& e)
    {
        UnregisterFromQueue();
        spdlog::error("{}", e.what());
        return false;
    }
    catch (...)
    {
        UnregisterFromQueue();
        spdlog::error("Unknown non-std::exception thrown inside Simulator::OnStateTransitionLocked.");
        return false;
    }

    spdlog::info("Simulator transitioned to new state. PlayerID: {} State: {}", _playerID, std::to_underlying(newState));

    return true;
}

void Simulator::WorkerMain(std::stop_token stopToken)
{
    std::mutex tickWaitMutex;
    std::condition_variable_any tickWaitCV;

    T_Tick tick = _tickClock->GetCurrentTick();

    while (!stopToken.stop_requested())
    {
        try
        {
            auto queue = _queue.lock();
            if (!queue)
            {
                spdlog::error("Simulator failed to acquire queue inside its WorkerMain. Exitting now. Simulation ID: {}", _playerID);
                break;
            }
            
            if (const auto RandomEvent = CreateRandomEvent(tick); RandomEvent)
            {
                if (const auto result = queue->WaitAndPush(_queueRegistrationHandle.value(), RandomEvent.value(), tick, stopToken); !result)
                {
                    if (stopToken.stop_requested() && result.error() == QueueTypes::Error::operation_cancelled)
                    {
                        break;
                    }

                    spdlog::error("Simulator failed to push the created event to queue. Exitting this simulator.");
                    assert(false);
                    break;
                }
                else
                {
                    spdlog::debug("Simulator successfully pushed event to queue. PlayerID: {} EventID: {}", _playerID, RandomEvent->id);
                }
            }
            else if (!queue->UpdateSimulatorWatermark(_queueRegistrationHandle.value(), tick))
            {
                spdlog::error("Simulator failed to update queue with its latest watermark.");
                assert(false);
                break;
            }

            {
                std::unique_lock lock{ tickWaitMutex };

                (void)tickWaitCV.wait_until(
                    lock,
                    stopToken,
                    _tickClock->GetStartOfTick(tick + 1),
                    [this, tick]
                    {
                        return _tickClock->GetCurrentTick() > tick;
                    });
            }

            tick = _tickClock->GetCurrentTick();

        }
        catch (const std::exception& e)
        {
            spdlog::error("{}", e.what());
            break;
        }
        catch (...)
        {
            spdlog::error("Unknown non-std::exception thrown inside Simulator::WorkerMain.");
            break;
        }
    }

    SwitchToState(SimulatorTypes::TSimulatorStateMachineState::Stopped);

    UnregisterFromQueue();
}

void Simulator::UnregisterFromQueue()
{
    try
    {
        if (auto queue = _queue.lock())
        {
            if (_queueRegistrationHandle)
            {
                if (const auto result = queue->UnRegisterSimulator(_queueRegistrationHandle.value()); !result)
                {
                    spdlog::error("Failed to unregister simulator from queue. PlayerID: {}", _playerID);
                }
            }
        }
    }
    catch (const std::exception& e)
    {
        spdlog::error("{}", e.what());
    }
    catch (...)
    {
        spdlog::error("Unknown non-std::exception thrown inside Simulator::UnregisterFromQueue.");
    }

    _queue.reset();
    _queueRegistrationHandle.reset();
}
