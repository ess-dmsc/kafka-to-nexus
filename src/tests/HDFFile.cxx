#include <unistd.h>
#include <gtest/gtest.h>
#include "../src/helper.h"
#include "../src/CommandHandler.h"
#include "../src/HDFFile.h"
#include "../src/HDFFile_impl.h"
#include "../src/SchemaRegistry.h"
#include "../src/schemas/f141/synth.h"
#include "../src/KafkaW.h"
#include <rapidjson/document.h>

// Verify
TEST(HDFFile, create) {
	auto fname = "tmp-test.h5";
	unlink(fname);
	using namespace BrightnESS::FileWriter;
	HDFFile f1;
	f1.init("tmp-test.h5", rapidjson::Value());
}


class T_HDFFile : public testing::Test {
public:
static void write_f141() {
	auto fname = "tmp-test.h5";
	auto source_name = "some-sourcename";
	unlink(fname);
	using namespace BrightnESS;
	using namespace BrightnESS::FileWriter;
	FileWriter::HDFFile f1;
	f1.init(fname, rapidjson::Value());
	auto & reg = BrightnESS::FileWriter::Schemas::SchemaRegistry::items();
	std::array<char, 4> fbid {{ 'f', '1', '4', '1' }};
	auto writer = reg.at(fbid)->create_reader()->create_writer();
	BrightnESS::FileWriter::Msg msg;
	{
		BrightnESS::FlatBufs::f141_epics_nt::synth synth(source_name, BrightnESS::FlatBufs::f141_epics_nt::PV::NTScalarArrayDouble, 1, 1);
		auto fb = synth.next(0);
		msg = BrightnESS::FileWriter::Msg {(char*)fb.builder->GetBufferPointer(), (int32_t)fb.builder->GetSize()};
	}
	// f1.impl->h5file
	writer->init(&f1, source_name, msg);
}
};

TEST_F(T_HDFFile, write_f141) {
	T_HDFFile::write_f141();
}


class T_CommandHandler : public testing::Test {
public:
static void new_03() {
	auto fname = "tmp-new-03.h5";
	auto source_name = "some-sourcename";
	unlink(fname);
	using namespace BrightnESS;
	using namespace BrightnESS::FileWriter;
	auto cmd = gulp("test/msg-cmd-new-03.json");
	LOG(3, "cmd: {:.{}}", cmd.data(), cmd.size());
	FileWriter::CommandHandler ch(nullptr);
	ch.handle({cmd.data(), (int32_t)cmd.size()});
	return;

	FileWriter::HDFFile f1;
	f1.init(fname, rapidjson::Value());
	auto & reg = BrightnESS::FileWriter::Schemas::SchemaRegistry::items();
	std::array<char, 4> fbid {{ 'f', '1', '4', '1' }};
	auto writer = reg.at(fbid)->create_reader()->create_writer();
	BrightnESS::FileWriter::Msg msg;
	{
		BrightnESS::FlatBufs::f141_epics_nt::synth synth(source_name, BrightnESS::FlatBufs::f141_epics_nt::PV::NTScalarArrayDouble, 1, 1);
		auto fb = synth.next(0);
		msg = BrightnESS::FileWriter::Msg {(char*)fb.builder->GetBufferPointer(), (int32_t)fb.builder->GetSize()};
	}
	// f1.impl->h5file
	writer->init(&f1, source_name, msg);
}
};

TEST_F(T_CommandHandler, new_03) {
	T_CommandHandler::new_03();
}
