module;

#include "hamed_common/generic_types.h"
#include "hamed_common/subscription_registry.h"

export module game_pulse.queue;

import game_pulse.domain;

import <concepts>;
import <condition_variable>;
import <cstddef>;
import <expected>;
import <type_traits>;
import <unordered_map>;
import <span>;


template<typename T>
concept NothrowQueuePayload =
    std::is_object_v<T> &&
    std::destructible<T> &&
    std::is_nothrow_move_constructible_v<T> &&
    std::is_nothrow_move_assignable_v<T>;

export
{

    namespace QueueTypes
    {

        enum class Error
        {
            bad_arguments = 0,
            internal_error,
            queue_not_started_or_shut_down,
            operation_cancelled,
            regressing_watermark_passed,
            simulator_already_registered,
            simulator_not_registered,
        };
    }

    class Queue final : public TStateMachine<>
    {
    public:

        using TSimulatorHandle = std::uint64_t;

        explicit Queue(const std::size_t queue_capacity);

        [[nodiscard]] std::size_t GetSize() const;
        [[nodiscard]] std::size_t GetCapacity() const noexcept { return _events_queue.capacity(); }

        std::expected<TSimulatorHandle, QueueTypes::Error> RegisterSimulator(T_ID simulatorID);
        std::expected<void, QueueTypes::Error> UnRegisterSimulator(const TSimulatorHandle& handle);

        std::expected<void, QueueTypes::Error> WaitAndPush(TSimulatorHandle simulatorHandle, EventTypes::Event event, T_Tick completedThroughTick, std::stop_token stopToken);
        std::expected<std::span<EventTypes::Event>, QueueTypes::Error> WaitAndPop(std::span<EventTypes::Event> destination, T_Tick throughTick, std::stop_token stopToken);

        // Separate call than WaitAndPush in case a simulator doesn't have any events to push but would like to update the queue's watermark to unblock it
        std::expected<void, QueueTypes::Error> UpdateSimulatorWatermark(TSimulatorHandle simulatorHandle, T_Tick completedThroughTick);

    protected:
        
        virtual bool OnStateTransitionLocked(const TStateMachineState newState) noexcept override;
        virtual void OnStateTransitionUnlocked(const TStateMachineState newState) noexcept override;

    private:

        struct SimulatorEntry
        {
            T_ID simulatorID;
            std::shared_ptr<std::optional<T_Tick>> completedThroughTick;
        };

        static_assert(NothrowQueuePayload<EventTypes::Event> && std::is_trivially_copyable_v<EventTypes::Event>, "Queue requires a trivially copyable & nothrow-movable Event payload.");

        std::condition_variable_any _queue_push_cv;
        std::condition_variable_any _queue_pop_cv;

        TRingQueue<EventTypes::Event> _events_queue;

        inline static constexpr std::string_view SubscriptionRegistryKey = "QueueSimulators";

        TSubscriptionRegistry<SimulatorEntry, std::string_view, TSimulatorHandle> _subscriptionRegistry;

        // Returns whether or not the value has advanced
        bool UpdateSimulatorWatermarkUnlocked(TSimulatorHandle simulatorHandle, T_Tick completedThroughTick);
        T_Tick GetSimulatorsThroughTick() const;

    };

}
