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
            virtual void processEventsSynchronously(const std::span<const EventTypes::Event>& events) = 0;
            virtual ~ProcessorInterface() = default;
        };

        enum class Error
        {
            BadArguments = 0,
            ProcessorAlreadyRegistered,
            ProcessorNotRegistered,
        };
    }

    class Pipeline final : public TStateMachine<>
    {
    public:

        using TProcessorHandle = std::uint64_t;

        explicit Pipeline(std::shared_ptr<TickClock> tickClock, std::shared_ptr<Queue> queue, const std::size_t batchSize);
        ~Pipeline();

        std::expected<TProcessorHandle, PipelineTypes::Error> registerProcessor(std::shared_ptr<PipelineTypes::ProcessorInterface> processor);
        std::expected<void, PipelineTypes::Error> unRegisterProcessor(const TProcessorHandle& handle);

        void joinAndWait();

    protected:

        virtual bool onStateTransitionLocked(const TStateMachineState newState) noexcept override;

    private:

        std::shared_ptr<TickClock> tickClock_;
        std::weak_ptr<Queue> queue_;
        std::size_t batchSize_;

        inline static constexpr std::string_view kSubscriptionRegistryKey = "PipelineProcessors";

        TSubscriptionRegistry<std::weak_ptr<PipelineTypes::ProcessorInterface>, std::string_view, TProcessorHandle> subscriptionRegistry_;

        std::jthread workerThread_;

        void workerMain(std::stop_token stopToken);
    };
}
