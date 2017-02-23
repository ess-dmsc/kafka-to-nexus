#include "SchemaRegistry.h"
#include "HDFFile.h"
#include "HDFFile_h5.h"
#include <hdf5.h>
#include "schemas/amo0_psi_sinq_schema_generated.h"

#include <iterator>
#include <iostream>

namespace BrightnESS {
namespace FileWriter {
namespace Schemas {
namespace AMOR {

template <typename T> hid_t nat_type();
  //template <> hid_t nat_type<float>()    { return H5T_NATIVE_FLOAT; }
  //template <> hid_t nat_type<double>()   { return H5T_NATIVE_DOUBLE; }
template <> hid_t nat_type<uint32_t>() { return H5T_NATIVE_UINT32; }
template <> hid_t nat_type<uint64_t>() { return H5T_NATIVE_UINT64; }

class reader : public FBSchemaReader {
std::unique_ptr<FBSchemaWriter> create_writer_impl() override;
std::string sourcename_impl(Msg msg) override;
uint64_t ts_impl(Msg msg) override;
};


class writer : public FBSchemaWriter {
~writer() override;
void init_impl(std::string const & sourcename, Msg msg) override;
WriteResult write_impl(Msg msg) override;
  hid_t grp_event = -1;
  hid_t to,id,tz,ei; //datasets
  hid_t dsp = -1; // dataspace
  hid_t dpl = -1; // data properties list
  uint64_t pid = -1;
};

std::unique_ptr<FBSchemaWriter> reader::create_writer_impl() {
  return std::unique_ptr<FBSchemaWriter>(new writer);
}

std::string reader::sourcename_impl(Msg msg) {
  auto event = EventGenerator::FlatBufs::AMOR::GetEvent(msg.data);
  
  if (!event->htype()) {
    LOG(3, "WARNING message has no source name");
    return "";
  }
  return event->htype()->str();
}

// TODO
// Should be in64_t to handle error cases
uint64_t reader::ts_impl(Msg msg) {
  auto event = EventGenerator::FlatBufs::AMOR::GetEvent(msg.data);
  auto ts = event->ts();
  if (!ts) {
    LOG(3, "ERROR no time data sent");
    return 0;
  }
  return ts;
}




writer::~writer() {
}

void writer::init_impl(std::string const & sourcename, Msg msg) {
	// // TODO
	// // This is just a unbuffered, low-performance write.
	// // Add buffering after it works.
	auto file = hdf_file->h5file_detail().h5file();
	hsize_t ncols = 1000000;
	LOG(1, "amo0::init_impl  v.size() == {}", ncols);
	// NEEDS:
	// event_time_offset : event->data()[0:nelem-1]
	// event_id : event->data()[nelem:2*nelem-1]
	// event_time_zero : timestamp
	// event_index : ???
	
	grp_event = H5Gcreate1(file,"NXEvent_data",0);

	{
	  auto dt = nat_type<uint32_t>();
	  std::array<hsize_t, 1> sizes_ini {{0}};
	  std::array<hsize_t, 1> sizes_max {{H5S_UNLIMITED}};
	  this->dsp = H5Screate_simple(sizes_ini.size(), sizes_ini.data(), sizes_max.data());

	  if(true) {
	    LOG(0, "\tDataSpace isSimple {}", H5Sis_simple(dsp));
	    auto ndims = H5Sget_simple_extent_ndims(dsp);
	    LOG(0, "\tDataSpace getSimpleExtentNdims {}", ndims);
	    LOG(0, "\tDataSpace getSimpleExtentNpoints {}", 
	  	H5Sget_simple_extent_npoints(dsp));
	    std::vector<hsize_t> get_sizes_now;
	    std::vector<hsize_t> get_sizes_max;
	    get_sizes_now.resize(ndims);
	    get_sizes_max.resize(ndims);
	    H5Sget_simple_extent_dims(dsp, get_sizes_now.data(), get_sizes_max.data());
	    for (int i1 = 0; i1 < ndims; ++i1) {
	      LOG(0, "\tH5Sget_simple_extent_dims {:3} {:3}", 
	  	  get_sizes_now.at(i1), get_sizes_max.at(i1));
	    }
	  }

	  this->dpl = H5Pcreate(H5P_DATASET_CREATE);
	  std::array<hsize_t, 1> sizes_chk {{ncols}};
	  H5Pset_chunk(dpl, sizes_chk.size(), sizes_chk.data());
	  this->to = H5Dcreate1(grp_event,"event_time_offset", dt, dsp, dpl);
	  this->id = H5Dcreate1(grp_event,"event_id", dt, dsp, dpl);
	  H5Sclose(dsp);
	  H5Pclose(dpl);
	}
	{
	  auto dt = nat_type<uint64_t>();
	  std::array<hsize_t, 1> sizes_ini {{0}};
	  std::array<hsize_t, 1> sizes_max {{H5S_UNLIMITED}};
	  this->dsp = H5Screate_simple(sizes_ini.size(), sizes_ini.data(), sizes_max.data());
	  this->dpl = H5Pcreate(H5P_DATASET_CREATE);
	  std::array<hsize_t, 1> sizes_chk {{ncols}};
	  H5Pset_chunk(dpl, sizes_chk.size(), sizes_chk.data());
	  this->tz = H5Dcreate1(grp_event,"event_time_zero", dt, dsp, dpl);
	  this->ei = H5Dcreate1(grp_event,"event_index", dt, dsp, dpl);
	  H5Sclose(dsp);
	  H5Pclose(dpl);
	}

}


WriteResult writer::write_impl(Msg msg) {
  using A = std::array<hsize_t, 1>;

  auto event = EventGenerator::FlatBufs::AMOR::GetEvent(msg.data);
  if( (event->pid() != pid+1) && (pid < -1) ) {
    LOG(0, "amo0 stream event loss: {} -> {}", pid, event->pid());
    // TODO write into nexus log
  }
  pid = event->pid();


  A event_size = {{event->data()->size()/2}};
  auto dt = nat_type<uint32_t>();

  // later factor out
  auto & ds = this->to;
  auto tgt = H5Dget_space(ds);
  auto ndims = H5Sget_simple_extent_ndims(tgt);
  LOG(1, "DataSpace getSimpleExtentNdims {}", ndims);
  LOG(1, "DataSpace getSimpleExtentNpoints {}", 
      H5Sget_simple_extent_npoints(tgt));

  A get_sizes_now;
  A get_sizes_max;
  H5Sget_simple_extent_dims(tgt, get_sizes_now.data(), get_sizes_max.data());
  for (uint i1 = 0; i1 < get_sizes_now.size(); ++i1) {
    LOG(1, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
  }
  H5Sclose(tgt);

  A new_sizes(get_sizes_now);
  for (uint i1 = 0; i1 < new_sizes.size(); ++i1) {
    new_sizes.at(i1) += event_size.at(i1);
  }
  herr_t err = H5Dextend(ds, new_sizes.data());
  if (err < 0) {
    LOG(7, "ERROR can not extend dataset");
    return {-1};
  }

  tgt = H5Dget_space(ds);
  err = H5Sselect_hyperslab(tgt, H5S_SELECT_SET, get_sizes_now.data(), nullptr, 
			    event_size.data(), nullptr);
  if (err < 0) {
    LOG(7, "ERROR can not select mem hyperslab");
    return {-1};
  }
  auto mem = H5Screate_simple(event_size.size(), event_size.data(), nullptr);

  err = H5Dwrite(ds, dt, mem, tgt, H5P_DEFAULT, event->data()->data());
  if (err < 0) {
    LOG(7, "ERROR writing failed");
    return {-1};
  }

  H5Sclose(mem);
  H5Sclose(tgt);

  return {(int64_t)event->timestamp()};
}



class Info : public SchemaInfo {
public:
FBSchemaReader::ptr create_reader() override;
};

FBSchemaReader::ptr Info::create_reader() {
  return FBSchemaReader::ptr(new reader);
}


SchemaRegistry::Registrar<Info> g_registrar(fbid_from_str("amo0"));


}
}
}
}
