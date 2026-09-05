module;

#include <spdlog/spdlog.h>

module game_pulse.analytics;

import <cassert>;


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
        switch (currentEvent.type)
        {
        case EventTypes::EventType::Spawn:
        {
            spdlog::debug("Analytics processing event: [Spawn] PlayerID: {} Position: [{} , {}]", currentEvent.spawn.playerId, currentEvent.spawn.position[0], currentEvent.spawn.position[1]);
            
            const auto foundPlayer = _playersStatus.find(currentEvent.spawn.playerId);
            if (foundPlayer == _playersStatus.end() || foundPlayer->second.health == 0)
            {
                try
                {
                    _playersStatus[currentEvent.spawn.playerId] = { PlayerMaxHealth , currentEvent.spawn.position };
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
            break;
        }
        case EventTypes::EventType::Move:
        {
            spdlog::debug("Analytics processing event: [Move] PlayerID: {} Position: [{}, {}]", currentEvent.move.playerId, currentEvent.move.position[0], currentEvent.move.position[1]);

            const auto foundPlayer = _playersStatus.find(currentEvent.move.playerId);
            if (foundPlayer != _playersStatus.end() && foundPlayer->second.health > 0)
            {
                foundPlayer->second.position = currentEvent.move.position;
            }
            break;
        }
        case EventTypes::EventType::Shot:
        {
            spdlog::debug("Analytics processing event: [Shot] PlayerID: {} TargetID: {} Damage: {}", currentEvent.shot.shooterId, currentEvent.shot.targetId, currentEvent.shot.damage);

            const auto foundPlayer_Shooter = _playersStatus.find(currentEvent.shot.shooterId);
            const auto foundPlayer_Target = _playersStatus.find(currentEvent.shot.targetId);
            if (foundPlayer_Shooter != _playersStatus.end() && foundPlayer_Shooter->second.health > 0 &&
                foundPlayer_Target != _playersStatus.end() && foundPlayer_Target->second.health > 0)
            {
                foundPlayer_Target->second.health -= std::min(foundPlayer_Target->second.health, currentEvent.shot.damage);
            }
            break;
        }
        default:
            assert(false && "Unsupported event type passed to Analytics::ProcessEventsSynchronously.");
            break;
        }
    }
}
