
#include <algorithm>
#include <cassert>
#include <charconv>
#include <vector>
#include <memory>
#include <string_view>
#include <system_error>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>

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
                    return false;
                value = argv[index];
                return true;
            }

            if (argument.size() > option.size() && argument.starts_with(option)
                && argument[option.size()] == '=')
            {
                value = argument.substr(option.size() + 1);
                return true;
            }

            return false;
        };

        if (read_value("--queue-size"))
        {
            if (!parse_value(value, cfg->queue_capacity))
                return 2;
        }
        else if (read_value("--batch-size"))
        {
            if (!parse_value(value, cfg->batch_size))
                return 2;
        }
        else if (read_value("--player-count"))
        {
            if (!parse_value(value, cfg->player_count))
                return 2;
        }
        else if (read_value("--snapshot-interval"))
        {
            if (!parse_value(value, cfg->snapshot_interval))
                return 2;
        }
        else if (read_value("--shutdown-gracefully"))
        {
            if (value == "true" || value == "1")
                cfg->shutdown_gracefully = true;
            else if (value == "false" || value == "0")
                cfg->shutdown_gracefully = false;
            else
                return 2;
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
            assert(false && "Failed to register the analytics.");
            return 0;
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

        if (const auto result = queue->SwitchToState(TStateMachineState::InProgress); !result)
        {
            assert(false && "Failed to start the queue.");
            return 0;
        }

        for (auto& currentSimulator : simulators)
        {
            if (const auto result = currentSimulator->SwitchToState(SimulatorTypes::TSimulatorStateMachineState::InProgress); !result)
            {
                assert(false && "Failed to start simulator(s).");
                return 0;
            }
        }

        if (const auto result = reporting->SwitchToState(ReportingTypes::TReportingStateMachineState::InProgress); !result)
        {
            assert(false && "Failed to start reporting.");
            return 0;
        }

        if (const auto result = pipeline->SwitchToState(TStateMachineState::InProgress); !result)
        {
            assert(false && "Failed to start the pipeline.");
            return 0;
        }

        pipeline->JoinAndWait();
    }
    catch (const std::exception& e)
    {
        spdlog::error("{}", e.what());
        assert(false && "Failed to instantiate and/or start.");
        return 0;
    }
    catch (...)
    {
        spdlog::error("Unknown non-std::exception thrown inside main().");
        assert(false && "Failed to instantiate and/or start.");
        return 0;
    }

    spdlog::shutdown();

    return 1;
}
