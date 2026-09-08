module;

#include <spdlog/spdlog.h>

module game_pulse.analytics;

import <algorithm>;
import <cassert>;
import <type_traits>;
import <variant>;


AnalyticsTypes::AnalyticsSnapshot Analytics::getSnapshot() const
{
    std::shared_lock lock(mutex_);
    return AnalyticsTypes::AnalyticsSnapshot{ playersStatus_ };
}

void Analytics::processEventsSynchronously(const std::span<const EventTypes::Event>& events)
{
    if (events.empty())
    {
        return;
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);

    for (const auto& currentEvent : events)
    {
        std::visit([this](const auto& data)
            {
                using T = std::decay_t<decltype(data)>;

                if constexpr (std::is_same_v<T, EventTypes::SpawnEvent>)
                {
                    spdlog::debug("Analytics processing event: [Spawn] PlayerID: {} Position: [{} , {}]", data.playerId, data.position[0], data.position[1]);

                    const auto foundPlayer = playersStatus_.find(data.playerId);
                    if (foundPlayer == playersStatus_.end() || foundPlayer->second.health == 0)
                    {
                        try
                        {
                            playersStatus_[data.playerId] = { kPlayerMaxHealth , data.position };
                        }
                        catch (const std::exception& e)
                        {
                            spdlog::error("{}", e.what());
                            assert(false && "Allocation failed inside Analytics::processEventsSynchronously.");
                        }
                        catch (...)
                        {
                            spdlog::error("Unknown non-std::exception thrown inside Analytics::processEventsSynchronously.");
                            assert(false && "Allocation failed inside Analytics::processEventsSynchronously.");
                        }
                    }
                }
                else if constexpr (std::is_same_v<T, EventTypes::MoveEvent>)
                {
                    spdlog::debug("Analytics processing event: [Move] PlayerID: {} Position: [{}, {}]", data.playerId, data.position[0], data.position[1]);

                    const auto foundPlayer = playersStatus_.find(data.playerId);
                    if (foundPlayer != playersStatus_.end() && foundPlayer->second.health > 0)
                    {
                        foundPlayer->second.position = data.position;
                    }
                }
                else if constexpr (std::is_same_v<T, EventTypes::ShotEvent>)
                {
                    spdlog::debug("Analytics processing event: [Shot] PlayerID: {} TargetID: {} Damage: {}", data.shooterId, data.targetId, data.damage);

                    const auto foundPlayerShooter = playersStatus_.find(data.shooterId);
                    const auto foundPlayerTarget = playersStatus_.find(data.targetId);
                    if (foundPlayerShooter != playersStatus_.end() && foundPlayerShooter->second.health > 0 &&
                        foundPlayerTarget != playersStatus_.end() && foundPlayerTarget->second.health > 0)
                    {
                        foundPlayerTarget->second.health -= std::min(foundPlayerTarget->second.health, data.damage);
                    }
                }
                else
                {
                    spdlog::error("Unsupported event type passed to Analytics::processEventsSynchronously.");
                    assert(false && "Unsupported event type passed to Analytics::processEventsSynchronously.");
                }
            }, currentEvent.data);
    }
}
