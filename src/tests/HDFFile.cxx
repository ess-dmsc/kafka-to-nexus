#include <unistd.h>
#include <gtest/gtest.h>
#include "../helper.h"
#include "../CommandHandler.h"
#include "../HDFFile.h"
#include "../HDFFile_impl.h"
#include "../SchemaRegistry.h"
#include "../schemas/f141/synth.h"
#include "../schemas/ev42/ev42_synth.h"
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
	BrightnESS::FlatBufs::f141_epics_nt::synth synth(source_name, BrightnESS::FlatBufs::f141_epics_nt::PV::NTScalarArrayDouble, 1, 1);
	auto fb = synth.next<double>(0);
	msg = BrightnESS::FileWriter::Msg {(char*)fb.builder->GetBufferPointer(), (int32_t)fb.builder->GetSize()};
	// f1.impl->h5file
	writer->init(&f1, "/", source_name, msg);
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
	LOG(7, "cmd: {:.{}}", cmd.data(), cmd.size());
	rapidjson::Document d;
	d.Parse(cmd.data(), cmd.size());
	char const * fname = d["file_attributes"]["file_name"].GetString();
	//char const * source_name = d["streams"][0]["source"].GetString();
	unlink(fname);
	FileWriter::CommandHandler ch(nullptr);
	ch.handle({cmd.data(), (int32_t)cmd.size()});
}

static void new_03_data() {
	using namespace BrightnESS;
	using namespace BrightnESS::FileWriter;
	auto cmd = gulp("tests/msg-cmd-new-03.json");
	LOG(7, "cmd: {:.{}}", cmd.data(), cmd.size());
	rapidjson::Document d;
	d.Parse(cmd.data(), cmd.size());
	char const * fname = d["file_attributes"]["file_name"].GetString();
	char const * source_name = d["streams"][0]["source"].GetString();
	unlink(fname);
	FileWriter::CommandHandler ch(nullptr);
	ch.handle({cmd.data(), (int32_t)cmd.size()});

	ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)1);

	auto & fwt = ch.file_writer_tasks.at(0);
	ASSERT_EQ(fwt->demuxers().size(), (size_t)1);

	// TODO
	// Make demuxer process the test message.

	// From here, I need the file writer task instance
	return;

	auto & reg = BrightnESS::FileWriter::Schemas::SchemaRegistry::items();
	std::array<char, 4> fbid {{ 'f', '1', '4', '1' }};
	auto writer = reg.at(fbid)->create_reader()->create_writer();
	BrightnESS::FileWriter::Msg msg;
	BrightnESS::FlatBufs::f141_epics_nt::synth synth(source_name, BrightnESS::FlatBufs::f141_epics_nt::PV::NTScalarArrayDouble, 1, 1);
	auto fb = synth.next<double>(0);
	msg = BrightnESS::FileWriter::Msg {(char*)fb.builder->GetBufferPointer(), (int32_t)fb.builder->GetSize()};
}

static void data_ev42() {
	using namespace BrightnESS;
	using namespace BrightnESS::FileWriter;
	using std::array;
	using std::vector;
	auto cmd = gulp("tests/msg-cmd-new-03.json");
	LOG(7, "cmd: {:.{}}", cmd.data(), cmd.size());
	rapidjson::Document d;
	d.Parse(cmd.data(), cmd.size());
	char const * fname = d["file_attributes"]["file_name"].GetString();
	char const * source_name = d["streams"][0]["source"].GetString();
	unlink(fname);
	FileWriter::CommandHandler ch(nullptr);
	ch.handle({cmd.data(), (int32_t)cmd.size()});

	ASSERT_EQ(ch.file_writer_tasks.size(), (size_t)1);

	auto & fwt = ch.file_writer_tasks.at(0);
	ASSERT_EQ(fwt->demuxers().size(), (size_t)1);

	using DT = uint32_t;
	size_t const NN = 1894;
	int const SP = 1978;
	int seed = 1;
	auto & reg = BrightnESS::FileWriter::Schemas::SchemaRegistry::items();
	std::array<char, 4> fbid {{ 'e', 'v', '4', '2' }};
	auto writer = reg.at(fbid)->create_reader()->create_writer();
	BrightnESS::FileWriter::Msg msg;
	BrightnESS::FlatBufs::ev42::synth synth(source_name, NN, seed);
	for (int i1 = 0; i1 < SP; ++i1) {
		auto fb = synth.next();
		msg = BrightnESS::FileWriter::Msg {
			(char*)fb.builder->GetBufferPointer(),
			(int32_t)fb.builder->GetSize()
		};
		if (false) {
			auto v = binary_to_hex(msg.data, msg.size);
			LOG(7, "msg:\n{:.{}}", v.data(), v.size());
		}
		fwt->demuxers().at(0).process_message(msg.data, msg.size);
	}
	fwt.reset();

	herr_t err;

	auto fid = H5Fopen(fname, H5F_ACC_RDONLY, H5P_DEFAULT);
	ASSERT_GE(fid, 0);

	auto g0 = H5Gopen(fid, "/entry-01/instrument-01/events-01", H5P_DEFAULT);
	ASSERT_GE(g0, 0);

	auto ds = H5Dopen2(fid, "/entry-01/instrument-01/events-01/detector_id", H5P_DEFAULT);
	ASSERT_GE(ds, 0);

	auto dt = H5Dget_type(ds);
	ASSERT_GE(dt, 0);
	ASSERT_EQ(H5Tget_size(dt), sizeof(DT));

	auto dsp = H5Dget_space(ds);
	ASSERT_GE(dsp, 0);
	ASSERT_EQ(H5Sis_simple(dsp), 1);
	auto nn = H5Sget_simple_extent_npoints(dsp);
	ASSERT_EQ(nn, (int32_t)SP*NN);

	auto mem = H5Screate(H5S_SIMPLE);
	using A = array<hsize_t, 1>;
	A sini = {{ (uint32_t)nn }};
	A smax = {{ (uint32_t)nn }};
	H5Sset_extent_simple(mem, sini.size(), sini.data(), smax.data());

	vector<DT> data(nn);

	err = H5Dread(ds, H5T_NATIVE_UINT32, dsp, H5S_ALL, H5P_DEFAULT, data.data());
	ASSERT_GE(err, 0);

	{
		size_t n1 = 0;
		BrightnESS::FlatBufs::ev42::synth synth(source_name, NN, seed);
		for (int i1 = 0; i1 < SP; ++i1) {
			auto fb_ = synth.next();
			auto fb = fb_.root();
			auto a = fb->detector_id();
			for (size_t i2 = 0; i2 < a->size(); ++i2) {
				//LOG(3, "{}, {}, {}", i2, data.at(i2), a->Get(i2));
				ASSERT_EQ(a->Get(i2), data.at(n1));
				++n1;
			}
		}
		ASSERT_EQ(n1, SP*NN);
	}

	H5Tclose(dt);
	H5Dclose(ds);
	H5Gclose(g0);
	H5Fclose(fid);
}

};

TEST_F(T_CommandHandler, new_03) {
	T_CommandHandler::new_03();
}

TEST_F(T_CommandHandler, new_03_data) {
	T_CommandHandler::new_03_data();
}

TEST_F(T_CommandHandler, data_ev42) {
	T_CommandHandler::data_ev42();
}
