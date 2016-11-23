#pragma once

namespace BrightnESS {
namespace FileWriter {

// TODO
// Placeholder so far
class StreamID {
};

class MsgParser {
public:
/// Parses the packet to extract the information about which stream
/// this message will go to.
/// The returned StreamID could be directly a handle to the instance
/// which will handle the packet.
StreamID stream_id(int len, char * msg) = 0;
};

}
}
