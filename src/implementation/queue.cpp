module;

#include "hamed_common/generic_types.h"

#include <spdlog/spdlog.h>

module game_pulse.queue;

import game_pulse.simulator;

import <algorithm>;
import <cassert>;
import <limits>;
import <mutex>;
import <stdexcept>;


Queue::Queue(const std::size_t queue_capacity)
    : _events_queue { queue_capacity }
{
    if (queue_capacity == 0)
    {
        throw std::invalid_argument{ "Queue capacity must be greater than zero." };
    }
}

std::size_t Queue::GetSize() const
{
    std::lock_guard<std::mutex> lock(_state_mutex);
    return _events_queue.size();
}

std::expected<Queue::TSimulatorHandle, QueueTypes::Error> Queue::RegisterSimulator(T_ID simulatorID)
{
    TSimulatorHandle registeredHandle{};
    SimulatorEntry newSimulatorEntry{ std::move(simulatorID), std::make_shared<std::optional<T_Tick>>(std::nullopt) };

    {
        // Serialize duplicate checking and insertion so concurrent registrations
        // cannot register the same simulator ID. The registry locks each call
        // separately, so this sequence requires an outer lock.
        std::unique_lock lock{ _state_mutex };

        const bool foundSimulator = _subscriptionRegistry.forEachSubscribedObject(Queue::SubscriptionRegistryKey, [&newSimulatorEntry](const auto& currentSimulatorEntry)
            {
                return currentSimulatorEntry.simulatorID == newSimulatorEntry.simulatorID;
            });

        if (foundSimulator)
        {
            return std::unexpected{ QueueTypes::Error::simulator_already_registered };
        }

        registeredHandle = _subscriptionRegistry.subscribe(newSimulatorEntry, Queue::SubscriptionRegistryKey);
    }

    spdlog::info("Queue successfully registered simulator with handle: {}", registeredHandle);

    return { std::move(registeredHandle) };
}

std::expected<void, QueueTypes::Error> Queue::UnRegisterSimulator(const TSimulatorHandle& handle)
{
    const bool success = _subscriptionRegistry.unsubscribe(handle);
    spdlog::info("Queue's unregister call result: {} Handle: {}", success, handle);
    return success ? std::expected<void, QueueTypes::Error>{} : std::unexpected{ QueueTypes::Error::simulator_not_registered };
}

std::expected<void, QueueTypes::Error> Queue::WaitAndPush(TSimulatorHandle simulatorHandle, EventTypes::Event event, T_Tick completedThroughTick, std::stop_token stopToken)
{
    if (GetState() != TStateMachineState::InProgress)
    {
        return std::unexpected{ QueueTypes::Error::queue_not_started_or_shut_down };
    }

    {
        std::unique_lock<std::mutex> lock(_state_mutex);

        _queue_push_cv.wait(
            lock,
            stopToken,
            [this]
            {
                return GetState() != TStateMachineState::InProgress ||
                    !_events_queue.full();
            }
        );

        // Individual simulator cancellation.
        if (stopToken.stop_requested())
        {
            return std::unexpected{ QueueTypes::Error::operation_cancelled };
        }

        if (GetState() != TStateMachineState::InProgress)
        {
            return std::unexpected{ QueueTypes::Error::queue_not_started_or_shut_down };
        }

        if (!UpdateSimulatorWatermarkUnlocked(std::move(simulatorHandle), std::move(completedThroughTick)))
        {
            return std::unexpected{ QueueTypes::Error::regressing_watermark_passed };
        }

        if (_events_queue.try_emplace(std::move(event)))
        {
            // This is for debugging purposes only so worth the minor overhead
            if (_events_queue.full())
            {
                spdlog::warn("Queue has reached its capacity.");
            }
        }
        else
        {
            spdlog::warn("Broken internal logic as _events_queue should not be full and std::move should work on event objects.");
            assert(false && "Broken internal logic as _events_queue should not be full and std::move should work on event objects.");
            return std::unexpected{ QueueTypes::Error::internal_error };
        }
    }

    _queue_pop_cv.notify_one();

    return {};
}

std::expected<std::span<EventTypes::Event>, QueueTypes::Error> Queue::WaitAndPop(std::span<EventTypes::Event> destination, T_Tick throughTick, std::stop_token stopToken)
{
    if (GetState() != TStateMachineState::InProgress)
    {
        return std::unexpected{ QueueTypes::Error::queue_not_started_or_shut_down };
    }
    
    if (destination.size() == 0)
    {
        return std::unexpected{ QueueTypes::Error::bad_arguments };
    }

    std::unique_lock lock{ _state_mutex };

    _queue_pop_cv.wait(
        lock,
        stopToken,
        [this, &throughTick]
        {
            return (!_events_queue.empty() && GetSimulatorsThroughTick() >= throughTick) || GetState() == TStateMachineState::Stopped;
        }
    );

    // Individual simulator cancellation.
    if (stopToken.stop_requested())
    {
        return std::unexpected{ QueueTypes::Error::operation_cancelled };
    }

    const TStateMachineState cached_state = GetState();

    if (cached_state == TStateMachineState::Stopped)
    {
        return std::unexpected{ QueueTypes::Error::queue_not_started_or_shut_down };
    }

    const bool was_full = _events_queue.full();

    const auto retval_span = _events_queue.pop_into(destination, [&throughTick](const auto& event)
        {
            return event.tick <= throughTick;
        });
    const bool shouldStop = cached_state == TStateMachineState::Stopping_Gracefully && _events_queue.empty();

    if (shouldStop)
    {
        SwitchToStateLocked(lock, TStateMachineState::Stopped);
    }

    lock.unlock();

    if (shouldStop)
    {
        OnStateTransitionUnlocked(TStateMachineState::Stopped);
    }

    // Check whether or not we should notify all waiters
    if (cached_state == TStateMachineState::InProgress && was_full)
    {
        _queue_push_cv.notify_all();
    }

    return retval_span;
}

std::expected<void, QueueTypes::Error> Queue::UpdateSimulatorWatermark(TSimulatorHandle simulatorId, T_Tick completedThroughTick)
{
    {
        std::lock_guard lock{ _state_mutex };

        if (const auto cached_state = GetState(); cached_state != TStateMachineState::InProgress)
        {
            return std::unexpected{ QueueTypes::Error::queue_not_started_or_shut_down };
        }

        if (!UpdateSimulatorWatermarkUnlocked(simulatorId, completedThroughTick))
        {
            return std::unexpected{ QueueTypes::Error::regressing_watermark_passed };
        }
    }

    _queue_pop_cv.notify_one();

    return {};
}

bool Queue::OnStateTransitionLocked(const TStateMachineState newState) noexcept
{
    if (!TStateMachine::OnStateTransitionLocked(newState))
    {
        return false;
    }

    if (newState == TStateMachineState::Stopped)
    {
        _events_queue.clear();
    }

    spdlog::info("Queue transitioned to new state. State: {}", std::to_underlying(newState));

    return true;
}

void Queue::OnStateTransitionUnlocked(const TStateMachineState newState) noexcept
{
    if (newState == TStateMachineState::Stopping_Gracefully || newState == TStateMachineState::Stopped)
    {
        _queue_push_cv.notify_all();
        _queue_pop_cv.notify_all();
    }
}

bool Queue::UpdateSimulatorWatermarkUnlocked(TSimulatorHandle simulatorHandle, T_Tick completedThroughTick)
{
    if (auto foundSimulator = _subscriptionRegistry.getSubscribedObject(Queue::SubscriptionRegistryKey, simulatorHandle); foundSimulator)
    {
        if (foundSimulator->completedThroughTick->value_or(T_Tick{}) > completedThroughTick)
        {
            return false;
        }

        foundSimulator->completedThroughTick->emplace(completedThroughTick);

        spdlog::debug("Queue successfully updated simulator's watermark. SimulatorId: {} Watermark: {}", std::move(foundSimulator->simulatorID), std::move(completedThroughTick));

        return true;
    }
    return false;
}

T_Tick Queue::GetSimulatorsThroughTick() const
{
    T_Tick throughTick{ std::numeric_limits<T_Tick>::max() };

    (void)_subscriptionRegistry.forEachSubscribedObject(Queue::SubscriptionRegistryKey, [&throughTick](const auto& currentSimulatorEntry)
        {
            if (!currentSimulatorEntry.completedThroughTick || !currentSimulatorEntry.completedThroughTick->has_value())
            {
                throughTick = T_Tick{};
                return true;
            }
            throughTick = std::min(throughTick, currentSimulatorEntry.completedThroughTick->value());
            return false;
        });

    return throughTick;
}
