module;

#include "hamed_common/generic_types.h"
#include "hamed_common/platform.h"

#include <spdlog/spdlog.h>

module game_pulse.pipeline;

import game_pulse.queue;

import <cassert>;


Pipeline::Pipeline(std::shared_ptr<TickClock> tickClock, std::shared_ptr<Queue> queue, const std::size_t batchSize)
    : tickClock_(std::move(tickClock)), queue_(queue), batchSize_(batchSize)
{
    if (!tickClock_ || !queue || batchSize_ == 0)
    {
        throw std::invalid_argument{ "Invalid tickClock, queue or batchSize passed to Pipeline's ctor." };
    }
    if (batchSize_ > queue->getCapacity())
    {
        spdlog::warn("Batch size bigger than queue's capacity is not useful.");
        assert(false && "Batch size bigger than queue's capacity is not useful.");
    }
}

Pipeline::~Pipeline()
{
    switchToState(TStateMachineState::Stopped);
}

std::expected<Pipeline::TProcessorHandle, PipelineTypes::Error> Pipeline::registerProcessor(std::shared_ptr<PipelineTypes::ProcessorInterface> processor)
{
    if (!processor)
    {
        return std::unexpected{ PipelineTypes::Error::BadArguments };
    }

    TProcessorHandle registeredHandle{};

    {
        std::unique_lock lock{ stateMutex_ };

        const bool foundElement = subscriptionRegistry_.forEachSubscribedObject(Pipeline::kSubscriptionRegistryKey, [&processor](auto& currentProcessor)
            {
                return pointersHaveSameControlBlock(currentProcessor, processor);
            });

        if (foundElement)
        {
            return std::unexpected{ PipelineTypes::Error::ProcessorAlreadyRegistered };
        }

        registeredHandle = subscriptionRegistry_.subscribe(processor, Pipeline::kSubscriptionRegistryKey);
    }

    spdlog::info("Pipeline successfully registered processor.");

    return { std::move(registeredHandle) };
}

std::expected<void, PipelineTypes::Error> Pipeline::unRegisterProcessor(const TProcessorHandle& handle)
{
    const bool success = subscriptionRegistry_.unsubscribe(handle);
    spdlog::info("Pipeline's unregister call result: {} Handle: {}", success, handle);
    return success ? std::expected<void, PipelineTypes::Error>{} : std::unexpected{ PipelineTypes::Error::ProcessorNotRegistered };
}

void Pipeline::joinAndWait()
{
    if (workerThread_.joinable())
    {
        workerThread_.join();
    }
}

bool Pipeline::onStateTransitionLocked(const TStateMachineState newState) noexcept
{
    if (!TStateMachine::onStateTransitionLocked(newState))
    {
        return false;
    }

    try
    {
        if (newState == TStateMachineState::InProgress)
        {
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
        spdlog::error("{}", e.what());
        return false;
    }
    catch (...)
    {
        spdlog::error("Unknown non-std::exception thrown inside Pipeline::onStateTransitionLocked.");
        return false;
    }

    spdlog::info("Pipeline transitioned to new state. State: {}", std::to_underlying(newState));

    return true;
}

void Pipeline::workerMain(std::stop_token stopToken)
{
    std::vector<EventTypes::Event> eventsBatchBuffer(batchSize_, {});

    // Whether a stop is graceful or not is decided upstream by the Queue (it either
    // drains to empty before reporting queueNotStartedOrShutDown below, or clears
    // immediately) - Pipeline itself just keeps pumping until Queue says there's nothing
    // left, so it needs no StoppingGracefully state of its own.
    while (!stopToken.stop_requested())
    {
        const TickClock::Tick targetTick = tickClock_->getCurrentTick();

        auto queue = queue_.lock();
        if (!queue)
        {
            spdlog::error("Pipeline failed to acquire queue inside its workerMain. Exiting now");
            break;
        }
        const auto expectedEvents = queue->waitAndPop(eventsBatchBuffer, targetTick, stopToken);
        queue.reset();

        if (!expectedEvents)
        {
            switch (expectedEvents.error())
            {
            case QueueTypes::Error::BadArguments:
                spdlog::error("Pipeline passed invalid arguments to Queue::waitAndPop.");
                assert(false && "Pipeline passed invalid arguments to Queue::waitAndPop.");
                break;

            case QueueTypes::Error::InternalError:
                spdlog::error("Queue::waitAndPop encountered an internal error.");
                assert(false && "Queue::waitAndPop encountered an internal error.");
                break;

            case QueueTypes::Error::QueueNotStartedOrShutDown:
                // Expected during shutdown: the queue has either finished draining
                // (graceful stop) or was cleared immediately (non-graceful stop).
                // Either way, there is nothing left for this pipeline to do.
                spdlog::info("Queue has shut down. Pipeline is stopping.");
                break;

            case QueueTypes::Error::OperationCancelled:
                if (!stopToken.stop_requested())
                {
                    spdlog::error("Broken logic and contract between Pipeline & Queue.");
                    assert("Broken logic and contract between Pipeline & Queue.");
                }
                break;

            default:
                spdlog::error("Unsupported QueueTypes::Error found inside Pipeline::workerMain.");
                assert(false && "Unsupported QueueTypes::Error found inside Pipeline::workerMain.");
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

        // Events already arrive chronological: Queue::waitAndPop sorts by (tick, id)
        // before popping, so no need to re-sort here.

        const auto subscribers = subscriptionRegistry_.getSubscribedObjects(Pipeline::kSubscriptionRegistryKey);

        if (!subscribers)
        {
            spdlog::error("Failed to fetch subscribers' list inside Pipeline::workerMain. Exiting pipeline loop.");
            assert(false && "Failed to fetch subscribers' list inside Pipeline::workerMain. Exiting pipeline loop.");
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
                    lockedSubscriber->processEventsSynchronously(expectedEvents.value());
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
            subscriptionRegistry_.removeSubscribedObjectsIf(Pipeline::kSubscriptionRegistryKey, [](const auto& currentProcessor)
                {
                    return currentProcessor.expired();
                });
        }

        HAMEDSEYF_CPU_RELAX();
    }

    if (getState() != TStateMachineState::Stopped)
    {
        if (const auto stopResult = switchToState(TStateMachineState::Stopped); !stopResult)
        {
            spdlog::error("Pipeline failed to transition to Stopped state on its final thread exit.");
            assert(false && "Pipeline failed to transition to Stopped state on its final thread exit.");
        }
    }
}
