module;

#include <spdlog/spdlog.h>

module game_pulse.analytics;

import <algorithm>;
import <cassert>;
import <type_traits>;
import <variant>;


AnalyticsType::AnalyticsSnapshot Analytics::GetSnapshot() const
{
    std::shared_lock lock(_mutex);
    return AnalyticsType::AnalyticsSnapshot{ _playersStatus };
}

void Analytics::ProcessEventsSynchronously(const std::span<const EventTypes::Event>& events)
{
    if (events.empty())
    {
        return;
    }
    
    std::unique_lock<std::shared_mutex> lock(_mutex);

    for (const auto& currentEvent : events)
    {
        std::visit([this](const auto& data)
            {
                using T = std::decay_t<decltype(data)>;

                if constexpr (std::is_same_v<T, EventTypes::SpawnEvent>)
                {
                    spdlog::debug("Analytics processing event: [Spawn] PlayerID: {} Position: [{} , {}]", data.playerId, data.position[0], data.position[1]);

                    const auto foundPlayer = _playersStatus.find(data.playerId);
                    if (foundPlayer == _playersStatus.end() || foundPlayer->second.health == 0)
                    {
                        try
                        {
                            _playersStatus[data.playerId] = { PlayerMaxHealth , data.position };
                        }
                        catch (const std::exception& e)
                        {
                            spdlog::error("{}", e.what());
                            assert(false && "Allocation failed inside Analytics::ProcessEventsSynchronously.");
                        }
                        catch (...)
                        {
                            spdlog::error("Unknown non-std::exception thrown inside Analytics::ProcessEventsSynchronously.");
                            assert(false && "Allocation failed inside Analytics::ProcessEventsSynchronously.");
                        }
                    }
                }
                else if constexpr (std::is_same_v<T, EventTypes::MoveEvent>)
                {
                    spdlog::debug("Analytics processing event: [Move] PlayerID: {} Position: [{}, {}]", data.playerId, data.position[0], data.position[1]);

                    const auto foundPlayer = _playersStatus.find(data.playerId);
                    if (foundPlayer != _playersStatus.end() && foundPlayer->second.health > 0)
                    {
                        foundPlayer->second.position = data.position;
                    }
                }
                else if constexpr (std::is_same_v<T, EventTypes::ShotEvent>)
                {
                    spdlog::debug("Analytics processing event: [Shot] PlayerID: {} TargetID: {} Damage: {}", data.shooterId, data.targetId, data.damage);

                    const auto foundPlayer_Shooter = _playersStatus.find(data.shooterId);
                    const auto foundPlayer_Target = _playersStatus.find(data.targetId);
                    if (foundPlayer_Shooter != _playersStatus.end() && foundPlayer_Shooter->second.health > 0 &&
                        foundPlayer_Target != _playersStatus.end() && foundPlayer_Target->second.health > 0)
                    {
                        foundPlayer_Target->second.health -= std::min(foundPlayer_Target->second.health, data.damage);
                    }
                }
                else
                {
                    spdlog::error("Unsupported event type passed to Analytics::ProcessEventsSynchronously.");
                    assert(false && "Unsupported event type passed to Analytics::ProcessEventsSynchronously.");
                }
            }, currentEvent.data);
    }
}
