#pragma once

namespace BrightnESS {
namespace FileWriter {

class TopicReader {
public:
  /// Called with a new, so far unidentified message.
  /// Extracts the format (only Flatbuffers so far).
  /// Hands on to the parser.
  void handle_message(int len, char *msg);
};

} // namespace FileWriter
} // namespace BrightnESS
