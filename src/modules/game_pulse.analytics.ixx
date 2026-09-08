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

    namespace AnalyticsTypes
    {
        // Using std::map so the reported logs appear sorted based on player IDs
        using TPlayerStatusMap = std::map<TId, SnapshotTypes::PlayerStatus>;

        struct AnalyticsSnapshot
        {
            TPlayerStatusMap playersStatus;
        };
    }

    class Analytics : public PipelineTypes::ProcessorInterface
    {
    public:

        [[nodiscard]] virtual AnalyticsTypes::AnalyticsSnapshot getSnapshot() const;

        // PipelineTypes::ProcessorInterface override(s)
        virtual void processEventsSynchronously(const std::span<const EventTypes::Event>& events) override;

    protected:

        mutable std::shared_mutex mutex_;

        AnalyticsTypes::TPlayerStatusMap playersStatus_;
    };

}
