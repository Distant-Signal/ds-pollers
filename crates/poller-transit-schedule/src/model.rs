//! What a source adapter produces (transit spec §6.2): one network's
//! timetable as the `transit_*` rows, before `NaPTAN` names and rail links
//! (2.3) are applied and it is split into stream parts.

use common::transit::{
    Horizon, TransitLine, TransitPattern, TransitServiceDays, TransitStop, TransitTrip,
};

/// A parsed, filtered and folded timetable.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TransitModel {
    pub lines: Vec<TransitLine>,
    /// Stations, then platforms (and stops with no station, such as a
    /// replacement bus's), each sorted by id.
    pub stops: Vec<TransitStop>,
    pub patterns: Vec<TransitPattern>,
    /// Sorted by trip id.
    pub trips: Vec<TransitTrip>,
    /// Only services with a date in the horizon.
    pub service_days: Vec<TransitServiceDays>,
    pub horizon: Horizon,
    /// The source's own version (`feed_info.txt`), if any.
    pub source_feed_version: Option<String>,
}

impl TransitModel {
    /// Service dates summed over services (the commit's `service_days`).
    pub(crate) fn service_day_count(&self) -> usize {
        self.service_days.iter().map(|s| s.dates.len()).sum()
    }
}
