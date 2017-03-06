#include <unistd.h>
#include <gtest/gtest.h>
#include "../helper.h"
#include "../CommandHandler.h"
#include "../HDFFile.h"
#include "../HDFFile_impl.h"
#include "../SchemaRegistry.h"
#include "../schemas/f141/synth.h"
#include "../KafkaW.h"
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
	using namespace BrightnESS;
	using namespace BrightnESS::FileWriter;
	auto cmd = gulp("tests/msg-cmd-new-03.json");
	LOG(3, "cmd: {:.{}}", cmd.data(), cmd.size());
	rapidjson::Document d;
	d.Parse(cmd.data(), cmd.size());
	char const * fname = d["file_attributes"]["file_name"].GetString();
	char const * source_name = d["streams"][0]["source"].GetString();
	unlink(fname);
	FileWriter::CommandHandler ch(nullptr);
	ch.handle({cmd.data(), (int32_t)cmd.size()});

	// From here, I need the file writer task instance
	return;

	auto & reg = BrightnESS::FileWriter::Schemas::SchemaRegistry::items();
	std::array<char, 4> fbid {{ 'f', '1', '4', '1' }};
	auto writer = reg.at(fbid)->create_reader()->create_writer();
	BrightnESS::FileWriter::Msg msg;
	{
		BrightnESS::FlatBufs::f141_epics_nt::synth synth(source_name, BrightnESS::FlatBufs::f141_epics_nt::PV::NTScalarArrayDouble, 1, 1);
		auto fb = synth.next(0);
		msg = BrightnESS::FileWriter::Msg {(char*)fb.builder->GetBufferPointer(), (int32_t)fb.builder->GetSize()};
	}
}
};

TEST_F(T_CommandHandler, new_03) {
	T_CommandHandler::new_03();
}
