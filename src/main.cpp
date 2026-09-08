
#include <algorithm>
#include <cassert>
#include <charconv>
#include <chrono>
#include <csignal>
#include <thread>
#include <vector>
#include <memory>
#include <string_view>
#include <system_error>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>

#include "hamed_common/platform.h"

import game_pulse.analytics;
import game_pulse.domain;
import game_pulse.pipeline;
import game_pulse.queue;
import game_pulse.reporting;
import game_pulse.simulator;

template <typename T>
concept ChronoDuration =
    requires
{
    typename T::rep;
    typename T::period;
};

namespace
{
    // Set from a signal handler, so it must stay a plain sig_atomic_t: a signal handler
    // may only touch a small set of async-signal-safe operations, and a lock-free atomic
    // isn't guaranteed to be one of them. Polling it from ordinary thread context (below)
    // keeps every real shutdown action out of the handler itself.
    volatile std::sig_atomic_t g_ShutdownRequested = 0;

    extern "C" void HandleShutdownSignal(int) noexcept
    {
        g_ShutdownRequested = 1;
    }
}

int main(int argc, char** argv)
{
    std::shared_ptr<Configuration> cfg = std::make_shared<Configuration>();

    // Parsing the passed arguments to derive Configuration
    const auto parse_value = []<typename T>(std::string_view value, T& destination) noexcept
    {
        if constexpr (ChronoDuration<T>)
        {
            typename T::rep count{};

            const auto [end, error] = std::from_chars(
                value.data(),
                value.data() + value.size(),
                count);

            if (error != std::errc{} || end != value.data() + value.size())
            {
                return false;
            }

            destination = T{ count };

            return true;
        }
        else
        {
            const auto [end, error] = std::from_chars(
                value.data(),
                value.data() + value.size(),
                destination);

            return error == std::errc{} && end == value.data() + value.size();
        }
    };

    for (int index = 1; index < argc; ++index)
    {
        const std::string_view argument{argv[index]};
        std::string_view value;

        const auto read_value = [&](std::string_view option) noexcept {
            if (argument == option)
            {
                if (++index == argc)
                {
                    return false;
                }
                value = argv[index];
                return true;
            }

            if (argument.size() > option.size() && argument.starts_with(option) && argument[option.size()] == '=')
            {
                value = argument.substr(option.size() + 1);
                return true;
            }

            return false;
        };

        if (read_value("--queue-size"))
        {
            if (!parse_value(value, cfg->queue_capacity))
            {
                return 2;
            }
        }
        else if (read_value("--batch-size"))
        {
            if (!parse_value(value, cfg->batch_size))
            {
                return 2;
            }
        }
        else if (read_value("--player-count"))
        {
            if (!parse_value(value, cfg->player_count))
            {
                return 2;
            }
        }
        else if (read_value("--snapshot-interval"))
        {
            if (!parse_value(value, cfg->snapshot_interval))
            {
                return 2;
            }
        }
        else if (read_value("--shutdown-gracefully"))
        {
            if (value == "true" || value == "1")
            {
                cfg->shutdown_gracefully = true;
            }
            else if (value == "false" || value == "0")
            {
                cfg->shutdown_gracefully = false;
            }
            else
            {
                return 2;
            }
        }
        else
        {
            return 2;
        }
    }

    auto logger = spdlog::basic_logger_mt("gamepulse", "gamepulse.log", true);
    spdlog::set_default_logger(std::move(logger));
    spdlog::set_level(spdlog::level::debug);
    spdlog::flush_every(std::chrono::milliseconds{ 500 });
    spdlog::flush_on(spdlog::level::err);

    try
    {
        std::shared_ptr<TickClock> tickClock = std::make_shared<TickClock>(cfg->tick_duration);
        std::shared_ptr<Analytics> analytics = std::make_shared<Analytics>();
        std::shared_ptr<Reporting> reporting = std::make_shared<Reporting>(analytics, cfg->snapshot_interval);
        std::shared_ptr<Queue> queue = std::make_shared<Queue>(cfg->queue_capacity);
        std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>(tickClock, queue, cfg->batch_size);

        if (const auto result = pipeline->RegisterProcessor(analytics); !result)
        {
            spdlog::critical("Failed to register the analytics.");
            assert(false && "Failed to register the analytics.");
            return 1;
        }

        /* Simulators for player related events */
        std::vector<T_ID> playerIDs(cfg->player_count);
        std::generate(
            playerIDs.begin(),
            playerIDs.end(),
            []()
            {
                return GlobalID::NextID();
            });      

        const SimulatorTypes::TEventGenerationWeights eventGenerationWeights{
            .spawnWeight = 0.10,
            .moveWeight = 0.40,
            .shotWeight = 0.30,
            .noEventWeight = 0.20,
        };
        constexpr std::uint64_t masterSimulatorSeed = 0x5EED'2026ULL;

        std::vector<std::shared_ptr<Simulator>> simulators;
        simulators.reserve(cfg->player_count);

        for (const auto currentPlayerId : playerIDs)
        {
            auto otherPlayerIDs = playerIDs;

            std::erase(otherPlayerIDs, currentPlayerId);

            std::shared_ptr<Simulator> simulator = std::make_shared<Simulator>
                (
                    tickClock,
                    queue,
                    currentPlayerId,
                    otherPlayerIDs,
                    eventGenerationWeights,
                    masterSimulatorSeed + currentPlayerId
                );

            simulators.push_back(simulator);
        }

        if (const auto result = queue->SwitchToState(QueueTypes::TStateMachineState::InProgress); !result)
        {
            spdlog::critical("Failed to start the queue.");
            assert(false && "Failed to start the queue.");
            return 1;
        }

        for (auto& currentSimulator : simulators)
        {
            if (const auto result = currentSimulator->SwitchToState(TStateMachineState::InProgress); !result)
            {
                spdlog::critical("Failed to start simulator(s).");
                assert(false && "Failed to start simulator(s).");
                return 1;
            }
        }

        if (const auto result = reporting->SwitchToState(TStateMachineState::InProgress); !result)
        {
            spdlog::critical("Failed to start reporting.");
            assert(false && "Failed to start reporting.");
            return 1;
        }

        if (const auto result = pipeline->SwitchToState(TStateMachineState::InProgress); !result)
        {
            spdlog::critical("Failed to start the pipeline.");
            assert(false && "Failed to start the pipeline.");
            return 1;
        }

        std::signal(SIGINT, HandleShutdownSignal);
        std::signal(SIGTERM, HandleShutdownSignal);

        spdlog::info("GamePulse is running. Send SIGINT/SIGTERM (e.g. Ctrl+C) to shut down {}.", cfg->shutdown_gracefully ? "gracefully" : "immediately");

        while (!g_ShutdownRequested && pipeline->GetState() == TStateMachineState::InProgress)
        {
            HAMEDSEYF_SPIN_OR_SLEEP_MS(false, 50);
        }

        if (g_ShutdownRequested)
        {
            spdlog::info("Shutdown signal received. Beginning orderly shutdown.");
        }
        else
        {
            spdlog::warn("Pipeline stopped on its own; see prior log entries for the cause. Shutting down the rest of the system.");
        }

        // Producers first: stop and fully join every simulator so none of them can push another event or hold a stale watermark, before deciding what happens to whatever they already queued.
        for (auto& currentSimulator : simulators)
        {
            currentSimulator->SwitchToState(TStateMachineState::Stopped);
        }
        for (auto& currentSimulator : simulators)
        {
            currentSimulator->JoinAndWait();
        }

        // A graceful stop drains whatever is left in the queue to the pipeline before finishing; a non-graceful stop clears the queue immediately and drops it.
        queue->SwitchToState(cfg->shutdown_gracefully ? QueueTypes::TStateMachineState::Stopping_Gracefully : QueueTypes::TStateMachineState::Stopped);

        // Pipeline notices the queue has shut down (drained or cleared) and stops itself.
        pipeline->JoinAndWait();

        reporting->SwitchToState(TStateMachineState::Stopped);
        reporting->JoinAndWait();

        spdlog::info("Shutdown complete.");
    }
    catch (const std::exception& e)
    {
        spdlog::error("{}", e.what());
        assert(false && "Failed to instantiate and/or start.");
        return 1;
    }
    catch (...)
    {
        spdlog::error("Unknown non-std::exception thrown inside main().");
        assert(false && "Failed to instantiate and/or start.");
        return 1;
    }

    spdlog::shutdown();

    return 0;
}
