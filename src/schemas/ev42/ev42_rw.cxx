#include "../../SchemaRegistry.h"
#include "../../HDFFile.h"
#include "../../HDFFile_h5.h"
#include <hdf5.h>
#include "schemas/ev42_events_generated.h"

namespace BrightnESS {
namespace FileWriter {
namespace Schemas {
namespace ev42 {


class reader : public FBSchemaReader {
std::unique_ptr<FBSchemaWriter> create_writer_impl() override;
std::string sourcename_impl(Msg msg) override;
uint64_t ts_impl(Msg msg) override;
};


class writer : public FBSchemaWriter {
~writer() override;
void init_impl(std::string const & sourcename, hid_t hdf_group, Msg msg) override;
WriteResult write_impl(Msg msg) override;
using DT = double;
hid_t ds = -1;
hid_t dsp = -1;
hid_t dcpl = -1;
hid_t ds_seq_data = -1;
hid_t ds_seq_fwd = -1;
hid_t ds_ts_data = -1;
bool do_flush_always = true;
};


std::unique_ptr<FBSchemaWriter> reader::create_writer_impl() {
	return std::unique_ptr<FBSchemaWriter>(new writer);
}


uint64_t reader::ts_impl(Msg msg) {
	return 0;
}


std::string reader::sourcename_impl(Msg msg) {
	auto epicspv = GetEventMessage(msg.data);
	auto v = epicspv->source_name();
	if (!v) {
		LOG(4, "WARNING message has no source name");
		return "";
	}
	return v->str();
}


writer::~writer() {
	if (ds != -1) H5Dclose(ds);
	if (ds_seq_data != -1) H5Dclose(ds_seq_data);
	if (ds_seq_fwd != -1) H5Dclose(ds_seq_fwd);
	if (ds_ts_data != -1) H5Dclose(ds_ts_data);
	if (dsp != -1) H5Sclose(dsp);
	if (dcpl != -1) H5Pclose(dcpl);
}


void writer::init_impl(std::string const & sourcename, hid_t hdf_group, Msg msg) {
}


WriteResult writer::write_impl(Msg msg) {
	LOG(7, "ev42::write_impl");
	auto veri = flatbuffers::Verifier((uint8_t*)msg.data, msg.size);
	if (!VerifyEventMessageBuffer(veri)) {
		LOG(3, "bad flat buffer");
		return {-1};
	}
	int64_t ts = 1;
	return {ts};
}



class Info : public SchemaInfo {
public:
FBSchemaReader::ptr create_reader() override;
};

FBSchemaReader::ptr Info::create_reader() {
	return FBSchemaReader::ptr(new reader);
}

SchemaRegistry::Registrar<Info> g_registrar(fbid_from_str("ev42"));

}
}
}
}
