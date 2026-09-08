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

        void joinAndWait();

    protected:

        bool onStateTransitionLocked(const TStateMachineState newState) noexcept override;

    private:

        std::weak_ptr<const Analytics> analytics_;

        const std::chrono::milliseconds snapshotInterval_;

        mutable std::mutex tickWaitMutex_;
        std::condition_variable_any reportingWaitCv_;

        std::jthread workerThread_;

        void workerMain(std::stop_token stopToken);
    };
}
