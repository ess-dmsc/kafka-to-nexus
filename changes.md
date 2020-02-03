# Changes

*Add to release notes and clear this list when creating a new release*

- `use-hdf-swmr` and `abort-on-uninitialised-stream` are now CLI options rather than configurable per job in the Kafka start command.
- Sending status reports to Kafka is no longer optional.
- New style status reports are sent (DM-1800). This will require changes to NICOS.