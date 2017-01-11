#pragma once

namespace BrightnESS {
namespace FileWriter {

/// Interface, lets the Streamer convert a Kafka Message to a time difference.
/// This difference is negative if the message is older than what
/// this particular TimeDiffFromMessage is looking for.
/// The difference is given in milliseconds.
class TimeDifferenceFromMessage {
public:
virtual int64_t time_difference_from_message(void * msg_data, int msg_size) = 0;
};

}
}
