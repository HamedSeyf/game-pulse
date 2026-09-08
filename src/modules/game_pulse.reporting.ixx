module;

#include "hamed_common/generic_types.h"

export module game_pulse.reporting;

import game_pulse.analytics;

import <chrono>;
import <condition_variable>;
import <stop_token>;
import <thread>;


export
{

    class Reporting : public TStateMachine<>
    {
    public:

        explicit Reporting(std::shared_ptr<Analytics> analytics, std::chrono::milliseconds snapshotInterval);
        ~Reporting();

        void JoinAndWait();

    protected:

        bool OnStateTransitionLocked(const TStateMachineState newState) noexcept override;

    private:

        std::weak_ptr<const Analytics> _analytics;
        
        const std::chrono::milliseconds _snapshotInterval;

        mutable std::mutex _tickWaitMutex;
        std::condition_variable_any _reporting_wait_cv;

        std::jthread _workerThread;

        void WorkerMain(std::stop_token stopToken);
    };
}
