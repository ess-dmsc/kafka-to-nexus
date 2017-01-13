#include "FileWriterTask.h"
#include "Source.h"
#include "logger.h"

namespace BrightnESS {
namespace FileWriter {

using std::string;
using std::vector;

class FileWriterTask_impl {
void add_source(Source && source);
std::vector<Source> _sources;
friend class FileWriterTask;
friend class ::Test___FileWriterTask___Create01;
};

void FileWriterTask_impl::add_source(Source && source) {
	_sources.push_back(std::move(source));
}


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


}
}

#if HAVE_GTEST
#include <gtest/gtest.h>

class Test___FileWriterTask___Create01 {
public:
void run() {
	using namespace BrightnESS::FileWriter;
	FileWriterTask fwt;
	auto & i = *fwt.impl;
	//Source s1("dummy-topic", "dummy-sourcename");
	//i.add_source(std::move(s1));
	i.add_source({"dummy-topic", "dummy-sourcename"});
}
};

TEST(FileWriterTask, Create01) {
	Test___FileWriterTask___Create01().run();
}

#endif
