module;

#include "hamed_common/generic_types.h"

export module game_pulse.simulator;

import game_pulse.domain;
import game_pulse.queue;

import <cstdint>;
import <memory>;
import <optional>;
import <random>;
import <span>;
import <stop_token>;
import <thread>;
import <vector>;


export
{

    namespace SimulatorTypes
    {
        struct TEventGenerationWeights final
        {
            double spawnWeight{};
            double moveWeight{};
            double shotWeight{};
            double noEventWeight{};
        };
    }

    class Simulator : public TStateMachine<>
    {
    public:

        explicit Simulator(
            std::shared_ptr<TickClock> tickClock,
            std::shared_ptr<Queue> queue,
            TId playerId,
            const std::span<const TId> otherPlayerIds,
            SimulatorTypes::TEventGenerationWeights eventGenerationWeights,
            std::uint64_t randomSeed);
        ~Simulator();

        void joinAndWait();

    protected:

        bool onStateTransitionLocked(const TStateMachineState newState) noexcept override;

    private:

        struct TEventGenerationCutoffs final
        {
            double spawnEnd;
            double moveEnd;
            double shotEnd;
        };

        const TId playerId_;
        const std::vector<TId> otherPlayerIds_;

        std::shared_ptr<TickClock> tickClock_;

        std::weak_ptr<Queue> queue_;

        std::optional<Queue::TSimulatorHandle> queueRegistrationHandle_ = std::nullopt;

        const TEventGenerationCutoffs eventGenerationCutoffs_;

        std::mt19937_64 randomEngine_;
        std::uniform_real_distribution<double> unitDistribution_{ 0.0, 1.0 };
        std::uniform_int_distribution<std::size_t> targetDistribution_;
        std::uniform_int_distribution<TPlayerHealthType> damageDistribution_;

        std::jthread workerThread_;

        void workerMain(std::stop_token stopToken);

        [[nodiscard]] static TEventGenerationCutoffs buildEventGenerationCutoffs(const SimulatorTypes::TEventGenerationWeights& weights);
        [[nodiscard]] std::optional<EventTypes::Event> createRandomEvent(const TickClock::Tick tick);

        void unRegisterFromQueue();

    };
}
