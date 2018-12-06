#pragma once

namespace KafkaW {

class ConsumerSettings {
public:
  int OffsetsForTimesTimeoutMS = 1000;
  int MetadataTimeoutMS = 10000;
};
}
