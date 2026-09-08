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

    using TId = std::uint64_t;
    using TTick = std::uint64_t;
    using TPlayerHealthType = uint64_t;
    using TPlayerPositionType = std::array<double, 2>;
    inline constexpr TPlayerHealthType kPlayerMaxHealth = 10000;

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
            TId playerId;
            TPlayerPositionType position;
        };

        struct MoveEvent
        {
            TId playerId;
            TPlayerPositionType position;
        };

        struct ShotEvent
        {
            TId shooterId;
            TId targetId;
            TPlayerHealthType damage;
        };

        struct Event
        {
            TId id;
            TTick tick;

            std::variant<SpawnEvent, MoveEvent, ShotEvent> data;
        };
    }

    namespace SnapshotTypes
    {
        struct PlayerStatus
        {
            TPlayerHealthType health = kPlayerMaxHealth;
            TPlayerPositionType position{ 0.0, 0.0 };
        };
    }

    struct Configuration final
    {
        std::chrono::milliseconds tickDuration{40};
        size_t queueCapacity = 200;
        size_t batchSize = 10;
        size_t playerCount = 5;
        std::chrono::milliseconds snapshotInterval{500};
        bool shutdownGracefully = true;
    };

    class GlobalId
    {
    public:
        [[nodiscard]] inline static TId nextId() noexcept { return ++latestId_; };
    private:
        inline static std::atomic<TId> latestId_{ 0 };
    };

    class TickClock
    {
    public:
        using Tick = std::uint64_t;

        explicit TickClock(std::chrono::milliseconds tickDuration)
            : start_(std::chrono::steady_clock::now())
            , tickDuration_(tickDuration)
        {
            if (tickDuration <= std::chrono::milliseconds::zero())
            {
                throw std::invalid_argument{ "Invalid tickDuration passed to TickClock's ctor." };
            }
        }

        [[nodiscard]] Tick getCurrentTick() const
        {
            const auto elapsed = std::chrono::steady_clock::now() - start_;
            return std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() / tickDuration_.count();
        }

        [[nodiscard]] std::chrono::steady_clock::time_point getStartOfTick(const Tick tick) const noexcept
        {
            return start_ + tickDuration_ * static_cast<std::chrono::milliseconds::rep>(tick);
        }

    private:
        std::chrono::steady_clock::time_point start_;
        const std::chrono::milliseconds tickDuration_;
    };

}
