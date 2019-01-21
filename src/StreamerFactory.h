#include "DemuxTopic.h"
#include "StreamerOptions.h"

#include <memory>

namespace FileWriter {
class IStreamer;

class IStreamerFactory {
public:
  virtual std::unique_ptr<IStreamer> create(const std::string &Broker,
                                            DemuxTopic &Demux,
                                            const StreamerOptions &Opts) = 0;
  virtual ~IStreamerFactory() = default;
};

class KafkaStreamerFactory : public IStreamerFactory {
public:
  std::unique_ptr<IStreamer> create(const std::string &Broker,
                                    DemuxTopic &Demux,
                                    const StreamerOptions &Opts) override;
};

} // FileWriter
