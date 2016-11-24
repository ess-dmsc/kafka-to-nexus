#pragma once

#include <memory>
#include <rapidjson/document.h>

namespace BrightnESS {
namespace FileWriter {

/// Represents the streaming to one Nexus file.

/// Is created with a configuration and sets up the streamer objects.
/// Opens the actual HDF file(s) and passes the handles to the streamer objects.
/// We currently write to a single basic hdf file, so we open only one actual file so far.
class NexusWriter {
public:
NexusWriter(std::unique_ptr<rapidjson::Document> configuration);

/// Returns the current status which can be announced on the broker.
/// Return type still to be defined, should be a struct with the info we need.
int status();
};

}
}
