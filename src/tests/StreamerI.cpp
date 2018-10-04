#include "StreamerI.h"

class StubStreamer : public FileWriter::IStreamer {
public:
  using Error = FileWriter::Status::StreamerStatus;
  using Options = std::vector<std::string>;

  StubStreamer(const std::string &, const std::string &, const Options &,
               const Options &) {}
  ProcessMessageResult pollAndProcess(FileWriter::DemuxTopic &MessageProcessor)

      private : int n_src;
};
