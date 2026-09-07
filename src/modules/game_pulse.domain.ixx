export module game_pulse.domain;

import <array>;
import <atomic>;
import <chrono>;
import <cstddef>;
import <cstdint>;
import <stdexcept>;
import <variant>;


export
{

    using T_ID = std::uint64_t;
    using T_Tick = std::uint64_t;
    using TPlayerHealthType = uint64_t;
    using TPlayerPositionType = std::array<double, 2>;
    inline constexpr TPlayerHealthType PlayerMaxHealth = 10000;

    namespace EventTypes
    {
        enum class EventType : std::uint8_t
        {
            Spawn,
            Move,
            Shot,
        };

        struct SpawnEvent
        {
            T_ID playerId;
            TPlayerPositionType position;
        };

        struct MoveEvent
        {
            T_ID playerId;
            TPlayerPositionType position;
        };

        struct ShotEvent
        {
            T_ID shooterId;
            T_ID targetId;
            TPlayerHealthType damage;
        };

        struct Event
        {
            T_ID id;
            T_Tick tick;

            std::variant<SpawnEvent, MoveEvent, ShotEvent> data;
        };
    }

    namespace SnapshotTypes
    {
        struct PlayerStatus
        {
            TPlayerHealthType health = PlayerMaxHealth;
            TPlayerPositionType position{ 0.0, 0.0 };
        };
    }

    struct Configuration final
    {
        std::chrono::milliseconds tick_duration{40};
        size_t queue_capacity = 200;
        size_t batch_size = 10;
        size_t player_count = 5;
        std::chrono::milliseconds snapshot_interval{500};
        bool shutdown_gracefully = true;
    };

    class GlobalID
    {
    public:
        [[nodiscard]] inline static T_ID NextID() noexcept { return ++latestID; };
    private:
        inline static std::atomic<T_ID> latestID{ 0 };
    };

    class TickClock
    {
    public:
        using Tick = std::uint64_t;

        explicit TickClock(std::chrono::milliseconds tickDuration)
            : _start(std::chrono::steady_clock::now())
            , _tickDuration(tickDuration)
        {
            if (tickDuration <= std::chrono::milliseconds::zero())
            {
                throw std::invalid_argument{ "Invalid tickDuration passed to TickClock's ctor." };
            }
        }

        [[nodiscard]] Tick GetCurrentTick() const
        {
            const auto elapsed = std::chrono::steady_clock::now() - _start;
            return std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() / _tickDuration.count();
        }

        [[nodiscard]] std::chrono::steady_clock::time_point GetStartOfTick(const Tick tick) const noexcept
        {
            return _start + _tickDuration * static_cast<std::chrono::milliseconds::rep>(tick);
        }

    private:
        std::chrono::steady_clock::time_point _start;
        const std::chrono::milliseconds _tickDuration;
    };

}
