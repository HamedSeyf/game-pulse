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
    TId playerId,
    const std::span<const TId> otherPlayerIds,
    SimulatorTypes::TEventGenerationWeights eventGenerationWeights,
    std::uint64_t randomSeed
)
    :
    playerId_(playerId),
    otherPlayerIds_(otherPlayerIds.begin(), otherPlayerIds.end()),
    tickClock_(std::move(tickClock)),
    queue_(queue),
    eventGenerationCutoffs_(buildEventGenerationCutoffs(eventGenerationWeights)),
    randomEngine_(randomSeed),
    damageDistribution_{ 1, kPlayerMaxHealth }
{
    if (!tickClock_ || !queue || otherPlayerIds.empty())
    {
        throw std::invalid_argument{ "Invalid tickClock, queue or otherPlayerIds passed to Simulator's ctor." };
    }

    targetDistribution_ = std::uniform_int_distribution<std::size_t>{ 0, otherPlayerIds.size() - 1 };
}

Simulator::TEventGenerationCutoffs Simulator::buildEventGenerationCutoffs(const SimulatorTypes::TEventGenerationWeights& weights)
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

std::optional<EventTypes::Event> Simulator::createRandomEvent(const TickClock::Tick tick)
{
    const double sample = unitDistribution_(randomEngine_);

    const auto makeRandomPosition = [this]() -> TPlayerPositionType
        {
            return {
                unitDistribution_(randomEngine_),
                unitDistribution_(randomEngine_)
            };
        };

    if (sample < eventGenerationCutoffs_.spawnEnd)
    {
        return EventTypes::Event
        {
            .id = GlobalId::nextId(),
            .tick = tick,
            .data = EventTypes::SpawnEvent
            {
                .playerId = playerId_,
                .position = makeRandomPosition()
            }
        };
    }
    else if (sample < eventGenerationCutoffs_.moveEnd)
    {
        return EventTypes::Event
        {
            .id = GlobalId::nextId(),
            .tick = tick,
            .data = EventTypes::MoveEvent
            {
                .playerId = playerId_,
                .position = makeRandomPosition()
            }
        };
    }
    else if (sample < eventGenerationCutoffs_.shotEnd)
    {
        return EventTypes::Event
        {
            .id = GlobalId::nextId(),
            .tick = tick,
            .data = EventTypes::ShotEvent
            {
                .shooterId = playerId_,
                .targetId = otherPlayerIds_[targetDistribution_(randomEngine_)],
                .damage = damageDistribution_(randomEngine_)
            }
        };
    }

    return std::nullopt;
}

Simulator::~Simulator()
{
    switchToState(TStateMachineState::Stopped);
}

void Simulator::joinAndWait()
{
    if (workerThread_.joinable())
    {
        workerThread_.join();
    }
}

bool Simulator::onStateTransitionLocked(const TStateMachineState newState) noexcept
{
    if (!TStateMachine::onStateTransitionLocked(newState))
    {
        return false;
    }

    try
    {
        if (newState == TStateMachineState::InProgress)
        {
            auto queue = queue_.lock();
            if (!queue)
            {
                spdlog::error("Simulator failed to acquire queue on start. Rolling back state transition. PlayerID ID: {}", playerId_);
                return false;
            }

            if (const auto result = queue->registerSimulator(playerId_); result)
            {
                queueRegistrationHandle_ = result.value();
            }
            else
            {
                spdlog::error("Simulator failed to register with queue. Rolling back state transition. PlayerID: {}", playerId_);
                return false;
            }

            workerThread_ = std::jthread([this](std::stop_token stopToken)
                {
                    workerMain(stopToken);
                }
            );
        }
        else if (newState == TStateMachineState::Stopped)
        {
            workerThread_.request_stop();
        }
    }
    catch (const std::exception& e)
    {
        unRegisterFromQueue();
        spdlog::error("{}", e.what());
        return false;
    }
    catch (...)
    {
        unRegisterFromQueue();
        spdlog::error("Unknown non-std::exception thrown inside Simulator::onStateTransitionLocked.");
        return false;
    }

    spdlog::info("Simulator transitioned to new state. PlayerID: {} State: {}", playerId_, std::to_underlying(newState));

    return true;
}

void Simulator::workerMain(std::stop_token stopToken)
{
    std::mutex tickWaitMutex;
    std::condition_variable_any tickWaitCV;

    TTick tick = tickClock_->getCurrentTick();

    while (!stopToken.stop_requested())
    {
        try
        {
            auto queue = queue_.lock();
            if (!queue)
            {
                spdlog::error("Simulator failed to acquire queue inside its workerMain. Exiting now. PlayerID: {}", playerId_);
                break;
            }

            if (const auto randomEvent = createRandomEvent(tick); randomEvent)
            {
                if (const auto result = queue->waitAndPush(queueRegistrationHandle_.value(), randomEvent.value(), tick, stopToken); !result)
                {
                    if (stopToken.stop_requested() && result.error() == QueueTypes::Error::OperationCancelled)
                    {
                        break;
                    }

                    spdlog::error("Simulator failed to push the created event to queue. Exiting this simulator.");
                    assert(false);
                    break;
                }
                else
                {
                    spdlog::debug("Simulator successfully pushed event to queue. PlayerID: {} EventID: {}", playerId_, randomEvent->id);
                }
            }
            else if (!queue->updateSimulatorWatermark(queueRegistrationHandle_.value(), tick))
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
                    tickClock_->getStartOfTick(tick + 1),
                    [this, tick]
                    {
                        return tickClock_->getCurrentTick() > tick;
                    });
            }

            tick = tickClock_->getCurrentTick();

        }
        catch (const std::exception& e)
        {
            spdlog::error("{}", e.what());
            break;
        }
        catch (...)
        {
            spdlog::error("Unknown non-std::exception thrown inside Simulator::workerMain.");
            break;
        }
    }

    switchToState(TStateMachineState::Stopped);

    unRegisterFromQueue();
}

void Simulator::unRegisterFromQueue()
{
    try
    {
        if (auto queue = queue_.lock())
        {
            if (queueRegistrationHandle_)
            {
                if (const auto result = queue->unRegisterSimulator(queueRegistrationHandle_.value()); !result)
                {
                    spdlog::error("Failed to unregister simulator from queue. PlayerID: {}", playerId_);
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
        spdlog::error("Unknown non-std::exception thrown inside Simulator::unRegisterFromQueue.");
    }

    queue_.reset();
    queueRegistrationHandle_.reset();
}
