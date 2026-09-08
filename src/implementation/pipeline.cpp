module;

#include "hamed_common/generic_types.h"
#include "hamed_common/platform.h"

#include <spdlog/spdlog.h>

module game_pulse.pipeline;

import game_pulse.queue;

import <cassert>;


Pipeline::Pipeline(std::shared_ptr<TickClock> tickClock, std::shared_ptr<Queue> queue, const std::size_t batchSize)
    : _tickClock(std::move(tickClock)), _queue(queue), _batch_size(batchSize)
{
    if (!_tickClock || !queue || _batch_size == 0)
    {
        throw std::invalid_argument{ "Invalid tickClock, queue or batchSize passed to Pipeline's ctor." };
    }
    spdlog::warn("Batch size bigger than queue's capacity is not useful.");
    assert(_batch_size <= queue->GetCapacity() && "Batch size bigger than queue's capacity is not useful.");
}

Pipeline::~Pipeline()
{
    SwitchToState(TStateMachineState::Stopped);
}

std::expected<Pipeline::TProcessorHandle, PipelineTypes::Error> Pipeline::RegisterProcessor(std::shared_ptr<PipelineTypes::ProcessorInterface> processor)
{
    if (!processor)
    {
        return std::unexpected{ PipelineTypes::Error::bad_arguments };
    }

    TProcessorHandle registeredHandle{};

    {
        std::unique_lock lock{ _state_mutex };

        const bool foundElement = _subscriptionRegistry.forEachSubscribedObject(Pipeline::SubscriptionRegistryKey, [&processor](auto& currentProcessor)
            {
                return PointersHaveSameControlBlock(currentProcessor, processor);
            });

        if (foundElement)
        {
            return std::unexpected{ PipelineTypes::Error::processor_already_registered };
        }

        registeredHandle = _subscriptionRegistry.subscribe(processor, Pipeline::SubscriptionRegistryKey);
    }

    spdlog::info("Pipeline successfully registered processor.");

    return { std::move(registeredHandle) };
}

std::expected<void, PipelineTypes::Error> Pipeline::UnRegisterProcessor(const TProcessorHandle& handle)
{
    const bool success = _subscriptionRegistry.unsubscribe(handle);
    spdlog::info("Pipeline's unregister call result: {} Handle: {}", success, handle);
    return success ? std::expected<void, PipelineTypes::Error>{} : std::unexpected{ PipelineTypes::Error::processor_not_registered };
}

void Pipeline::JoinAndWait()
{
    if (_workerThread.joinable())
    {
        _workerThread.join();
    }
}

bool Pipeline::OnStateTransitionLocked(const TStateMachineState newState) noexcept
{
    if (!TStateMachine::OnStateTransitionLocked(newState))
    {
        return false;
    }

    try
    {
        if (newState == TStateMachineState::InProgress)
        {
            _workerThread = std::jthread([this](std::stop_token stopToken)
                {
                    WorkerMain(stopToken);
                }
            );
        }
        else if (newState == TStateMachineState::Stopped)
        {
            _workerThread.request_stop();
        }
    }
    catch (const std::exception& e)
    {
        spdlog::error("{}", e.what());
        return false;
    }
    catch (...)
    {
        spdlog::error("Unknown non-std::exception thrown inside Pipeline::OnStateTransitionLocked.");
        return false;
    }

    spdlog::info("Pipeline transitioned to new state. State: {}", std::to_underlying(newState));

    return true;
}

void Pipeline::WorkerMain(std::stop_token stopToken)
{
    std::vector<EventTypes::Event> eventsBatchBuffer(_batch_size, {});

    while (!stopToken.stop_requested())
    {
        const TickClock::Tick targetTick = _tickClock->GetCurrentTick();

        auto queue = _queue.lock();
        if (!queue)
        {
            spdlog::error("Pipeline failed to acquire queue inside its WorkerMain. Exiting now");
            break;
        }
        const auto expectedEvents = queue->WaitAndPop(eventsBatchBuffer, targetTick, stopToken);
        queue.reset();

        if (!expectedEvents)
        {
            switch (expectedEvents.error())
            {
            case QueueTypes::Error::bad_arguments:
                spdlog::error("Pipeline passed invalid arguments to Queue::WaitAndPopBatch.");
                assert(false && "Pipeline passed invalid arguments to Queue::WaitAndPopBatch.");
                break;

            case QueueTypes::Error::internal_error:
                spdlog::error("Queue::WaitAndPopBatch encountered an internal error.");
                assert(false && "Queue::WaitAndPopBatch encountered an internal error.");
                break;

            case QueueTypes::Error::queue_not_started_or_shut_down:
                // Expected during shutdown: the queue has either finished draining
                // (graceful stop) or was cleared immediately (non-graceful stop).
                // Either way, there is nothing left for this pipeline to do.
                spdlog::info("Queue has shut down. Pipeline is stopping.");
                break;

            case QueueTypes::Error::operation_cancelled:
                if (!stopToken.stop_requested())
                {
                    spdlog::error("Broken logic and contract between Pipeline & Queue.");
                    assert("Broken logic and contract between Pipeline & Queue.");
                }
                break;

            default:
                spdlog::error("Unsupported QueueTypes::Error found inside Pipeline::WorkerMain.");
                assert(false && "Unsupported QueueTypes::Error found inside Pipeline::WorkerMain.");
                break;
            }

            break;
        }

        spdlog::debug("Pipeline successfully popped {} events from the queue covering up to tick: {}", expectedEvents.value().size(), std::move(targetTick));

        if (expectedEvents.value().empty())
        {
            HAMEDSEYF_CPU_RELAX();
            continue;
        }

        // Events already arrive chronological: Queue::WaitAndPop sorts by (tick, id)
        // before popping, so no need to re-sort here.

        const auto subscribers = _subscriptionRegistry.getSubscribedObjects(Pipeline::SubscriptionRegistryKey);

        if (!subscribers)
        {
            spdlog::error("Failed to fetch subscribers' list inside Pipeline::WorkerMain. Exiting pipeline loop.");
            assert(false && "Failed to fetch subscribers' list inside Pipeline::WorkerMain. Exiting pipeline loop.");
            break;
        }

        // Since expiration is quite rare in current code's logic, it makes sense to potentially have another full pass on the subscription list and remove expired ones after the main for loop ends.
        bool hasExpiredValues = false;

        for (auto& currentSubscriber : subscribers.value())
        {
            try
            {
                if (auto lockedSubscriber = currentSubscriber.lock())
                {
                    lockedSubscriber->ProcessEventsSynchronously(expectedEvents.value());
                }
                else
                {
                    hasExpiredValues = true;
                }
            }
            catch (const std::exception& e)
            {
                spdlog::error("{}", e.what());
                assert(false && "Subscribers are supposed to gracefully handle events without throwing.");
            }
            catch (...)
            {
                spdlog::error("Unknown non-std::exception thrown by subscriber.");
                assert(false && "Subscribers are supposed to gracefully handle events without throwing.");
            }
        }

        if (hasExpiredValues)
        {
            _subscriptionRegistry.removeSubscribedObjectsIf(Pipeline::SubscriptionRegistryKey, [](const auto& currentProcessor)
                {
                    return currentProcessor.expired();
                });
        }

        HAMEDSEYF_CPU_RELAX();
    }

    if (GetState() != TStateMachineState::Stopped)
    {
        if (const auto stopResult = SwitchToState(TStateMachineState::Stopped); !stopResult)
        {
            spdlog::error("Pipeline failed to transition to Stopped state on its final thread exit.");
            assert(false && "Pipeline failed to transition to Stopped state on its final thread exit.");
        }
    }
}
