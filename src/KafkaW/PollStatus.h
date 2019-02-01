#pragma once
namespace KafkaW {
enum class PollStatus {
  Message,
  Error,
  EndOfPartition,
  Empty,
  TimedOut
};
}
