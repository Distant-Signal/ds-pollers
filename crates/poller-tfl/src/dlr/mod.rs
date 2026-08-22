//! DLR-specific arrivals-diffing pilot (see
//! `docs/superpowers/specs/2026-08-22-tfl-service-metrics-v2-design.md`,
//! Area 3, and `docs/superpowers/plans/2026-08-22-dlr-arrivals-diffing-pilot.md`).
//!
//! Unlike the rest of `poller-tfl`, which only relays status TfL has
//! already computed, this module infers `common::SampleStats` itself, by
//! diffing live Arrivals predictions against DLR's published Timetable for
//! one pilot station (Poplar). No other TfL line does this.

pub mod arrivals;
pub mod timetable;
pub mod inference;
