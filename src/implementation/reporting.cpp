module;

#include "hamed_common/platform.h"

#include <spdlog/spdlog.h>

module game_pulse.reporting;


Reporting::Reporting(std::shared_ptr<Analytics> analytics, std::chrono::milliseconds snapshotInterval)
    : analytics_(analytics), snapshotInterval_(snapshotInterval)
{
    if (!analytics || snapshotInterval <= std::chrono::milliseconds::zero())
    {
        throw std::invalid_argument{ "Invalid analytics and/or snapshotInterval passed to Reporting's ctor." };
    }
}

Reporting::~Reporting()
{
    switchToState(TStateMachineState::Stopped);
}

void Reporting::joinAndWait()
{
    if (workerThread_.joinable())
    {
        workerThread_.join();
    }
}

bool Reporting::onStateTransitionLocked(const TStateMachineState newState) noexcept
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
    catch ([[maybe_unused]] const std::exception& e)
    {
        SPDLOG_ERROR("{}", e.what());
        return false;
    }
    catch (...)
    {
        SPDLOG_ERROR("Unknown non-std::exception thrown inside Reporting::onStateTransitionLocked.");
        return false;
    }

    SPDLOG_INFO("Reporting transitioned to new state. State: {}", std::to_underlying(newState));

    return true;
}

void Reporting::workerMain(std::stop_token stopToken)
{
    while (!stopToken.stop_requested())
    {
        try
        {
            if (auto sharedAnalytics = analytics_.lock())
            {
                const auto snapShot = sharedAnalytics->getSnapshot();

#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_INFO
                for (const auto& currentPlayerDataPair : snapShot.playersStatus)
                {
                    SPDLOG_INFO(
                        "Snapshot report: Player {:>6} has {:>5} health and positioned at [{:>8.3f}, {:>8.3f}]",
                        currentPlayerDataPair.first,
                        currentPlayerDataPair.second.health,
                        currentPlayerDataPair.second.position[0],
                        currentPlayerDataPair.second.position[1]);
                }
#endif
            }
            else
            {
                SPDLOG_WARN("Analytics not found. Reporting would stop and exit now.");
                switchToState(TStateMachineState::Stopped);
                return;
            }
        }
        catch ([[maybe_unused]] const std::exception& e)
        {
            SPDLOG_ERROR("{}", e.what());
        }
        catch (...)
        {
            SPDLOG_ERROR("Unknown non-std::exception thrown inside Reporting::workerMain.");
        }

        std::chrono::steady_clock::time_point nextWakeUpTime = std::chrono::steady_clock::now() + snapshotInterval_;

        std::unique_lock lock{ tickWaitMutex_ };

        reportingWaitCv_.wait_until(
            lock,
            stopToken,
            nextWakeUpTime,
            [] { return false; }
        );
    }

    switchToState(TStateMachineState::Stopped);
}
