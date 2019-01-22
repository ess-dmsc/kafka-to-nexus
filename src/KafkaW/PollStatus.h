#pragma once
namespace KafkaW {
enum class PollStatus {
  Msg,
  Err,
  EOP,
  Empty,
};
}
