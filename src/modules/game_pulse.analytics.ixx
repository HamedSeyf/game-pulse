module;

#include "hamed_common/generic_types.h"

export module game_pulse.analytics;

import game_pulse.domain;
import game_pulse.pipeline;

import <shared_mutex>;
import <span>;
import <map>;


export
{

    namespace AnalyticsType
    {
        // Using std::map so the reported logs appear sorted based on player IDs
        using TPlayerStatusMap = std::map<T_ID, SnapshotTypes::PlayerStatus>;

        struct AnalyticsSnapshot
        {
            TPlayerStatusMap playersStatus;
        };
    }

    class Analytics : public PipelineTypes::ProcessorInterface
    {
    public:

        [[nodiscard]] virtual AnalyticsType::AnalyticsSnapshot GetSnapshot() const;

        // PipelineTypes::ProcessorInterface override(s)
        virtual void ProcessEventsSynchronously(const std::span<const EventTypes::Event>& events) override;

    protected:

        mutable std::shared_mutex _mutex;

        AnalyticsType::TPlayerStatusMap _playersStatus;
    };

}
