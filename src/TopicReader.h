#pragma once

namespace FileWriter {

class TopicReader {
public:
  /// \brief Called with a new, so far unidentified message.
  ///
  /// Extracts the format (only Flatbuffers so far).
  /// Passes to the parser.
  void handle_message(int len, char *msg);
};

} // namespace FileWriter
