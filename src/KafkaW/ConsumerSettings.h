#pragma once

namespace KafkaW {

struct ConsumerSettings {
  int OffsetsForTimesTimeoutMS = 1000;
  int MetadataTimeoutMS = 10000;
};
}
