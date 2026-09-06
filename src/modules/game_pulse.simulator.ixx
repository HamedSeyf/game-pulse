module;

#include "hamed_common/generic_types.h"

export module game_pulse.simulator;

import game_pulse.domain;
import game_pulse.queue;

import <memory>;
import <random>;
import <span>;
import <stop_token>;
import <thread>;
import <vector>;


export
{

    namespace SimulatorTypes
    {
        enum class TSimulatorStateMachineState
        {
            NotStarted = 0,
            InProgress,
            Stopped,
        };

        struct TEventGenerationWeights final
        {
            double spawnWeight{};
            double moveWeight{};
            double shotWeight{};
            double noEventWeight{};
        };
    }

    class Simulator : public TStateMachine<SimulatorTypes::TSimulatorStateMachineState>
    {
    public:

        explicit Simulator(
            std::shared_ptr<TickClock> tickClock,
            std::shared_ptr<Queue> queue,
            T_ID playerID,
            const std::span<T_ID> otherPlayerIDs,
            SimulatorTypes::TEventGenerationWeights eventGenerationWeights,
            std::uint64_t randomSeed);

    protected:

        bool OnStateTransitionLocked(const SimulatorTypes::TSimulatorStateMachineState newState) noexcept override;

    private:

        struct TEventGenerationCutoffs final
        {
            double spawnEnd;
            double moveEnd;
            double shotEnd;
        };

        const T_ID _playerID;
        const std::vector<T_ID> _otherPlayerIDs;

        std::shared_ptr<TickClock> _tickClock;
            
        std::weak_ptr<Queue> _queue;

        std::optional<Queue::TSimulatorHandle> _queueRegistrationHandle = std::nullopt;

        const TEventGenerationCutoffs _eventGenerationCutoffs;

        std::mt19937_64 _randomEngine;
        std::uniform_real_distribution<double> _unitDistribution{ 0.0, 1.0 };
        std::uniform_int_distribution<std::size_t> _targetDistribution;
        std::uniform_int_distribution<TPlayerHealthType> _damageDistribution;

        std::jthread _workerThread;

        void WorkerMain(std::stop_token stopToken);

        [[nodiscard]] static TEventGenerationCutoffs BuildEventGenerationCutoffs(const SimulatorTypes::TEventGenerationWeights& weights);
        [[nodiscard]] std::optional<EventTypes::Event> CreateRandomEvent(const TickClock::Tick tick);

        void UnregisterFromQueue();

    };
}
