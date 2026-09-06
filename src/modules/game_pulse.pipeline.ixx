module;

#include "hamed_common/generic_types.h"
#include "hamed_common/subscription_registry.h"

export module game_pulse.pipeline;

import game_pulse.domain;
import game_pulse.queue;

import <cstddef>;
import <cstdint>;
import <expected>;
import <memory>;
import <span>;
import <string_view>;
import <thread>;
import <stop_token>;


export
{

    namespace PipelineTypes
    {
        class ProcessorInterface
        {
        public:
            // Processes events synchronously.
            // `events`, and any pointer/reference derived from it, are valid only for the
            // duration of this call. A processor that needs the events afterward must copy
            // them into processor-owned storage before returning.
            // events are sorted based on tick values and then their id values.
            virtual void ProcessEventsSynchronously(const std::span<const EventTypes::Event>& events) = 0;
            virtual ~ProcessorInterface() = default;
        };

        enum class Error
        {
            bad_arguments = 0,
            processor_already_registered,
            processor_not_registered,
        };
    }

    class Pipeline final : public TStateMachine<>
    {
    public:

        using TProcessorHandle = std::uint64_t;

        explicit Pipeline(std::shared_ptr<TickClock> tickClock, std::shared_ptr<Queue> queue, const std::size_t batchSize);
        ~Pipeline();

        std::expected<TProcessorHandle, PipelineTypes::Error> RegisterProcessor(std::shared_ptr<PipelineTypes::ProcessorInterface> processor);
        std::expected<void, PipelineTypes::Error> UnRegisterProcessor(const TProcessorHandle& handle);

        void JoinAndWait();

    protected:
        
        virtual bool OnStateTransitionLocked(const TStateMachineState newState) noexcept override;

    private:

        std::shared_ptr<TickClock> _tickClock;
        std::weak_ptr<Queue> _queue;
        std::size_t _batch_size;

        inline static constexpr std::string_view SubscriptionRegistryKey = "PipelineProcessors";

        TSubscriptionRegistry<std::weak_ptr<PipelineTypes::ProcessorInterface>, std::string_view, TProcessorHandle> _subscriptionRegistry;

        std::jthread _workerThread;

        void WorkerMain(std::stop_token stopToken);
    };
}
