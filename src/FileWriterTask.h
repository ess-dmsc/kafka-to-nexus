#pragma once
#include <string>
#include <vector>
#include <memory>
#include "DemuxTopic.h"

class Test___FileWriterTask___Create01;

namespace BrightnESS {
namespace FileWriter {

class FileWriterTask_impl;

/**
Represents the task of writing a HDF file.
It contains the list of Source and DemuxTopic
and makes those available to the FileMaster and Streamer.
Created by Master on command message and passed to FileMaster in ctor.
*/
class FileWriterTask {
friend class ::Test___FileWriterTask___Create01;
public:
FileWriterTask();
/// Used by Streamer to get the list of demuxers
std::vector<DemuxTopic> & demuxers();
private:
std::vector<DemuxTopic> _demuxers;
std::unique_ptr<FileWriterTask_impl> impl;
};

}
}
