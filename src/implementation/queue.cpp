module;

#include "hamed_common/generic_types.h"
#include "hamed_common/sorts.h"

#include <spdlog/spdlog.h>

module game_pulse.queue;

import game_pulse.simulator;

import <algorithm>;
import <cassert>;
import <limits>;
import <mutex>;
import <stdexcept>;
import <tuple>;


Queue::Queue(const std::size_t queueCapacity)
    : eventsQueue_ { queueCapacity }
{
    if (queueCapacity == 0)
    {
        throw std::invalid_argument{ "Queue capacity must be greater than zero." };
    }
}

std::size_t Queue::getSize() const
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    return eventsQueue_.size();
}

std::expected<Queue::TSimulatorHandle, QueueTypes::Error> Queue::registerSimulator(TId simulatorId)
{
    TSimulatorHandle registeredHandle{};
    SimulatorEntry newSimulatorEntry{ std::move(simulatorId), std::make_shared<std::optional<TTick>>(std::nullopt) };

    {
        // Serialize duplicate checking and insertion so concurrent registrations
        // cannot register the same simulator ID. The registry locks each call
        // separately, so this sequence requires an outer lock.
        std::unique_lock lock{ stateMutex_ };

        const bool foundSimulator = subscriptionRegistry_.forEachSubscribedObject(Queue::kSubscriptionRegistryKey, [&newSimulatorEntry](const auto& currentSimulatorEntry)
            {
                return currentSimulatorEntry.simulatorId == newSimulatorEntry.simulatorId;
            });

        if (foundSimulator)
        {
            return std::unexpected{ QueueTypes::Error::SimulatorAlreadyRegistered };
        }

        registeredHandle = subscriptionRegistry_.subscribe(newSimulatorEntry, Queue::kSubscriptionRegistryKey);
    }

    spdlog::info("Queue successfully registered simulator with handle: {}", registeredHandle);

    return { std::move(registeredHandle) };
}

std::expected<void, QueueTypes::Error> Queue::unRegisterSimulator(const TSimulatorHandle& handle)
{
    const bool success = subscriptionRegistry_.unsubscribe(handle);
    spdlog::info("Queue's unregister call result: {} Handle: {}", success, handle);
    return success ? std::expected<void, QueueTypes::Error>{} : std::unexpected{ QueueTypes::Error::SimulatorNotRegistered };
}

std::expected<void, QueueTypes::Error> Queue::waitAndPush(TSimulatorHandle simulatorHandle, EventTypes::Event event, TTick completedThroughTick, std::stop_token stopToken)
{
    if (getState() != QueueTypes::TStateMachineState::InProgress)
    {
        return std::unexpected{ QueueTypes::Error::QueueNotStartedOrShutDown };
    }

    {
        std::unique_lock<std::mutex> lock(stateMutex_);

        queuePushCv_.wait(
            lock,
            stopToken,
            [this]
            {
                return getState() != QueueTypes::TStateMachineState::InProgress || !eventsQueue_.full();
            }
        );

        // Individual simulator cancellation.
        if (stopToken.stop_requested())
        {
            return std::unexpected{ QueueTypes::Error::OperationCancelled };
        }

        if (getState() != QueueTypes::TStateMachineState::InProgress)
        {
            return std::unexpected{ QueueTypes::Error::QueueNotStartedOrShutDown };
        }

        if (!updateSimulatorWatermarkUnlocked(std::move(simulatorHandle), std::move(completedThroughTick)))
        {
            return std::unexpected{ QueueTypes::Error::RegressingWatermarkPassed };
        }

        if (eventsQueue_.try_emplace(std::move(event)))
        {
            // This is for debugging purposes only so worth the minor overhead
            if (eventsQueue_.full())
            {
                spdlog::warn("Queue has reached its capacity.");
            }
        }
        else
        {
            spdlog::warn("Broken internal logic as eventsQueue_ should not be full and std::move should work on event objects.");
            assert(false && "Broken internal logic as eventsQueue_ should not be full and std::move should work on event objects.");
            return std::unexpected{ QueueTypes::Error::InternalError };
        }
    }

    queuePopCv_.notify_one();

    return {};
}

std::expected<std::span<EventTypes::Event>, QueueTypes::Error> Queue::waitAndPop(std::span<EventTypes::Event> destination, TTick throughTick, std::stop_token stopToken)
{
    if (getState() != QueueTypes::TStateMachineState::InProgress)
    {
        return std::unexpected{ QueueTypes::Error::QueueNotStartedOrShutDown };
    }

    if (destination.size() == 0)
    {
        return std::unexpected{ QueueTypes::Error::BadArguments };
    }

    std::unique_lock lock{ stateMutex_ };

    queuePopCv_.wait(
        lock,
        stopToken,
        [this, &throughTick]
        {
            return (!eventsQueue_.empty() && getSimulatorsThroughTick() >= throughTick) || getState() == QueueTypes::TStateMachineState::Stopped;
        }
    );

    // Individual simulator cancellation.
    if (stopToken.stop_requested())
    {
        return std::unexpected{ QueueTypes::Error::OperationCancelled };
    }

    const auto cachedState = getState();

    if (cachedState == QueueTypes::TStateMachineState::Stopped)
    {
        return std::unexpected{ QueueTypes::Error::QueueNotStartedOrShutDown };
    }

    const bool wasFull = eventsQueue_.full();

    // Simulators push independently and can lag one another, so events can land in the
    // ring out of tick order. Sort in place before pop_into so what comes out is
    // chronological rather than push order.
    bubbleSort(eventsQueue_, [](const EventTypes::Event& lEvent, const EventTypes::Event& rEvent)
        {
            return std::tie(lEvent.tick, lEvent.id) < std::tie(rEvent.tick, rEvent.id);
        });

    const auto retvalSpan = eventsQueue_.pop_into(destination, [&throughTick](const auto& event)
        {
            return event.tick <= throughTick;
        });
    const bool shouldStop = cachedState == QueueTypes::TStateMachineState::StoppingGracefully && eventsQueue_.empty();

    if (shouldStop)
    {
        switchToStateLocked(lock, QueueTypes::TStateMachineState::Stopped);
    }

    lock.unlock();

    if (shouldStop)
    {
        onStateTransitionUnlocked(QueueTypes::TStateMachineState::Stopped);
    }

    // Check whether or not we should notify all waiters
    if (cachedState == QueueTypes::TStateMachineState::InProgress && wasFull)
    {
        queuePushCv_.notify_all();
    }

    return retvalSpan;
}

std::expected<void, QueueTypes::Error> Queue::updateSimulatorWatermark(TSimulatorHandle simulatorHandle, TTick completedThroughTick)
{
    {
        std::lock_guard lock{ stateMutex_ };

        if (const auto cachedState = getState(); cachedState != QueueTypes::TStateMachineState::InProgress)
        {
            return std::unexpected{ QueueTypes::Error::QueueNotStartedOrShutDown };
        }

        if (!updateSimulatorWatermarkUnlocked(simulatorHandle, completedThroughTick))
        {
            return std::unexpected{ QueueTypes::Error::RegressingWatermarkPassed };
        }
    }

    queuePopCv_.notify_one();

    return {};
}

bool Queue::onStateTransitionLocked(const QueueTypes::TStateMachineState newState) noexcept
{
    if (!TStateMachine::onStateTransitionLocked(newState))
    {
        return false;
    }

    if (newState == QueueTypes::TStateMachineState::Stopped)
    {
        eventsQueue_.clear();
    }

    spdlog::info("Queue transitioned to new state. State: {}", std::to_underlying(newState));

    return true;
}

void Queue::onStateTransitionUnlocked(const QueueTypes::TStateMachineState newState) noexcept
{
    if (newState == QueueTypes::TStateMachineState::StoppingGracefully || newState == QueueTypes::TStateMachineState::Stopped)
    {
        queuePushCv_.notify_all();
        queuePopCv_.notify_all();
    }
}

bool Queue::updateSimulatorWatermarkUnlocked(TSimulatorHandle simulatorHandle, TTick completedThroughTick)
{
    if (auto foundSimulator = subscriptionRegistry_.getSubscribedObject(Queue::kSubscriptionRegistryKey, simulatorHandle); foundSimulator)
    {
        if (foundSimulator->completedThroughTick->value_or(TTick{}) > completedThroughTick)
        {
            return false;
        }

        foundSimulator->completedThroughTick->emplace(completedThroughTick);

        spdlog::debug("Queue successfully updated simulator's watermark. SimulatorId: {} Watermark: {}", std::move(foundSimulator->simulatorId), std::move(completedThroughTick));

        return true;
    }
    return false;
}

TTick Queue::getSimulatorsThroughTick() const
{
    TTick throughTick{ std::numeric_limits<TTick>::max() };

    (void)subscriptionRegistry_.forEachSubscribedObject(Queue::kSubscriptionRegistryKey, [&throughTick](const auto& currentSimulatorEntry)
        {
            if (!currentSimulatorEntry.completedThroughTick || !currentSimulatorEntry.completedThroughTick->has_value())
            {
                throughTick = TTick{};
                return true;
            }
            throughTick = std::min(throughTick, currentSimulatorEntry.completedThroughTick->value());
            return false;
        });

    return throughTick;
}
