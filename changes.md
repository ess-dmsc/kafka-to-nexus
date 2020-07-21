# Changes

*Add to release notes and clear this list when creating a new release*

- Messages with the same timestamp, in the FlatBuffer, as the previous message are not written to file.
This feature is turned on for all writer modules except event messages (`ev42`). This allows the Filewriter
to ignore repeated updates from the Forwarder, which are sent to ensure there is an update available on Kafka
from shortly before the start of each file being written.
- The application is no longer (completely) blocking when doing initial set-up kafka meta data calls.
- Fixed bug which caused f142 messages with value of zero to be written as a different value.
