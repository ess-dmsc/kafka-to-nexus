#pragma once
#include <string>
#include <vector>
#include <memory>
#include "DemuxTopic.h"
#include "Source.h"
#include <rapidjson/document.h>

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
class FileWriterTask final {
friend class ::Test___FileWriterTask___Create01;
friend class CommandHandler;
public:
FileWriterTask();
~FileWriterTask();
FileWriterTask & set_hdf_filename(std::string hdf_filename);
/// Used by Streamer to get the list of demuxers
std::vector<DemuxTopic> & demuxers();
void file_flush();
void file_close();
private:
std::vector<DemuxTopic> _demuxers;
std::unique_ptr<FileWriterTask_impl> impl;
void add_source(Source && source);
/// Called by CommandHandler on setup.
int hdf_init(rapidjson::Value const & nexus_structure);
};

}
}
