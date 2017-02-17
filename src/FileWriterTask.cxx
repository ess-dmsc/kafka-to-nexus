#include "FileWriterTask.h"
#include "Source.h"
#include "logger.h"
#include "HDFFile.h"

namespace BrightnESS {
namespace FileWriter {

using std::string;
using std::vector;

class FileWriterTask_impl {
//void add_source(Source && source);
std::vector<Source> _sources;
friend class FileWriterTask;
friend class ::Test___FileWriterTask___Create01;
std::string hdf_filename;
HDFFile hdf_file;
};

/*
void FileWriterTask_impl::add_source(Source && source) {
	_sources.push_back(std::move(source));
}
*/


class SourceFactory_by_FileWriterTask {
private:
Source create(string topic, string sourcename);
friend class FileWriterTask;
};

Source SourceFactory_by_FileWriterTask::create(string topic, string sourcename) {
	return {topic, sourcename};
}

std::vector<DemuxTopic> & FileWriterTask::demuxers() {
	return _demuxers;
}


FileWriterTask::FileWriterTask() {
	impl.reset(new FileWriterTask_impl);
}

FileWriterTask::~FileWriterTask() {
}

FileWriterTask & FileWriterTask::set_hdf_filename(std::string hdf_filename) {
	impl->hdf_filename = hdf_filename;
	return *this;
}

void FileWriterTask::add_source(Source && source) {
	bool found = false;
	for (auto & d : _demuxers) {
		if (d.topic() == source.topic()) {
			d.add_source(std::move(source));
			found = true;
		}
	}
	if (!found) {
		_demuxers.emplace_back(source.topic());
		auto & d = _demuxers.back();
		d.add_source(std::move(source));
	}
}


int FileWriterTask::hdf_init() {
	auto x = impl->hdf_file.init(impl->hdf_filename);
	if (x) {
		return x;
	}
	for (auto & d: demuxers()) {
		for (auto & s : d.sources()) {
			s.hdf_init(impl->hdf_file);
		}
	}
}


void FileWriterTask::file_flush() {
	if (impl) impl->hdf_file.flush();
}


}
}

#if HAVE_GTEST
#include <gtest/gtest.h>

class Test___FileWriterTask___Create01 {
public:
void run() {
}
};

TEST(FileWriterTask, Create01) {
	Test___FileWriterTask___Create01().run();
}

#endif
