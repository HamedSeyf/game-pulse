module;

#include "hamed_common/generic_types.h"
#include "hamed_common/subscription_registry.h"

export module game_pulse.queue;

import game_pulse.domain;

import <concepts>;
import <condition_variable>;
import <cstddef>;
import <cstdint>;
import <expected>;
import <string_view>;
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

        enum class TStateMachineState
        {
            NotStarted = 0,
            InProgress,
            StoppingGracefully,
            Stopped,
        };

        enum class Error
        {
            BadArguments = 0,
            InternalError,
            QueueNotStartedOrShutDown,
            OperationCancelled,
            RegressingWatermarkPassed,
            SimulatorAlreadyRegistered,
            SimulatorNotRegistered,
        };
    }

    // TODO: [Future expansion] Every simulator happens to unregister cleanly on every exit path, so queue does not end up with stalled simulators;
    // However, the Queue class itself has no timeout or heartbeat protection against a producer that goes silent without unregistering.
    class Queue final : public TStateMachine<QueueTypes::TStateMachineState>
    {
    public:

        using TSimulatorHandle = std::uint64_t;

        explicit Queue(const std::size_t queueCapacity);

        [[nodiscard]] std::size_t getSize() const;
        [[nodiscard]] std::size_t getCapacity() const noexcept { return eventsQueue_.capacity(); }

        std::expected<TSimulatorHandle, QueueTypes::Error> registerSimulator(TId simulatorId);
        std::expected<void, QueueTypes::Error> unRegisterSimulator(const TSimulatorHandle& handle);

        std::expected<void, QueueTypes::Error> waitAndPush(TSimulatorHandle simulatorHandle, EventTypes::Event event, TTick completedThroughTick, std::stop_token stopToken);
        std::expected<std::span<EventTypes::Event>, QueueTypes::Error> waitAndPop(std::span<EventTypes::Event> destination, TTick throughTick, std::stop_token stopToken);

        // Separate call than waitAndPush in case a simulator doesn't have any events to push but would like to update the queue's watermark to unblock it
        std::expected<void, QueueTypes::Error> updateSimulatorWatermark(TSimulatorHandle simulatorHandle, TTick completedThroughTick);

    protected:

        virtual bool onStateTransitionLocked(const QueueTypes::TStateMachineState newState) noexcept override;
        virtual void onStateTransitionUnlocked(const QueueTypes::TStateMachineState newState) noexcept override;

    private:

        struct SimulatorEntry
        {
            TId simulatorId;
            std::shared_ptr<std::optional<TTick>> completedThroughTick;
        };

        static_assert(NothrowQueuePayload<EventTypes::Event> && std::is_trivially_copyable_v<EventTypes::Event>, "Queue requires a trivially copyable & nothrow-movable Event payload.");

        std::condition_variable_any queuePushCv_;
        std::condition_variable_any queuePopCv_;

        TRingQueue<EventTypes::Event> eventsQueue_;

        inline static constexpr std::string_view kSubscriptionRegistryKey = "QueueSimulators";

        TSubscriptionRegistry<SimulatorEntry, std::string_view, TSimulatorHandle> subscriptionRegistry_;

        // Returns whether or not the value has advanced
        bool updateSimulatorWatermarkUnlocked(TSimulatorHandle simulatorHandle, TTick completedThroughTick);
        TTick getSimulatorsThroughTick() const;

    };

}
