module;

#include "hamed_common/platform.h"

#include <spdlog/spdlog.h>

module game_pulse.reporting;


Reporting::Reporting(std::shared_ptr<Analytics> analytics, std::chrono::milliseconds snapshotInterval)
    : _analytics(analytics), _snapshotInterval(snapshotInterval)
{
    if (!analytics || snapshotInterval <= std::chrono::milliseconds::zero())
    {
        throw std::invalid_argument{ "Invalid analytics and/or snapshotInterval passed to Reporting's ctor." };
    }
}

void Reporting::OnStateTransitionLocked(const ReportingTypes::TReportingStateMachineState newState) noexcept
{
    spdlog::info("Reporting transitioned to new state. State: {}", std::to_underlying(newState));

    TStateMachine::OnStateTransitionLocked(newState);

    try
    {
        if (newState == ReportingTypes::TReportingStateMachineState::InProgress)
        {
            _workerThread = std::jthread([this](std::stop_token stopToken)
                {
                    WorkerMain(stopToken);
                }
            );
        }
        else if (newState == ReportingTypes::TReportingStateMachineState::Stopped)
        {
            _workerThread.request_stop();
        }
    }
    catch (const std::exception& e)
    {
        spdlog::error("{}", e.what());
    }
    catch (...)
    {
        spdlog::error("Unknown non-std::exception thrown inside Reporting::OnStateTransitionLocked.");
    }
}

void Reporting::WorkerMain(std::stop_token stopToken)
{
    while (!stopToken.stop_requested())
    {
        try
        {
            if (auto sharedAnalytics = _analytics.lock())
            {
                const auto snapShot = sharedAnalytics->GetSnapshot();

                for (const auto& currentPlayerDataPair : snapShot.playersStatus)
                {
                    spdlog::info(
                        "Snapshot report: Player {:>6} has {:>5} health and positioned at [{:>8.3f}, {:>8.3f}]",
                        currentPlayerDataPair.first,
                        currentPlayerDataPair.second.health,
                        currentPlayerDataPair.second.position[0],
                        currentPlayerDataPair.second.position[1]);
                }
            }
            else
            {
                spdlog::warn("Analytics not found. Reporting would stop and exit now.");
                SwitchToState(ReportingTypes::TReportingStateMachineState::Stopped);
                return;
            }
        }
        catch (const std::exception& e)
        {
            spdlog::error("{}", e.what());
        }
        catch (...)
        {
            spdlog::error("Unknown non-std::exception thrown inside Reporting::WorkerMain.");
        }

        std::chrono::steady_clock::time_point nextWakeUpTime = std::chrono::steady_clock::now() + _snapshotInterval;

        std::unique_lock lock{ _tickWaitMutex };

        _reporting_wait_cv.wait_until(
            lock,
            stopToken,
            nextWakeUpTime,
            [] { return false; }
        );
    }
}
