#pragma once

namespace FileWriter {

class StreamID {};

class MsgParser {
public:
  /// \brief Parses the packet to extract the information about which stream
  /// this message will go to.
  ///
  /// \param len
  /// \param msg
  ///
  /// \return StreamID could be directly passed to the instance which will
  /// handle the packet.
  StreamID stream_id(int len, char *msg) = 0;
};

} // namespace FileWriter
