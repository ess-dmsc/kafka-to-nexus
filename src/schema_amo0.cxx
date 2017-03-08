#include "SchemaRegistry.h"
#include "HDFFile.h"
#include "HDFFile_h5.h"
#include "schemas/amo0_psi_sinq_schema_generated.h"

#include <iterator>
#include <iostream>

namespace BrightnESS {
namespace FileWriter {
namespace Schemas {
namespace AMOR {

template <typename T> hid_t nat_type();
template <> hid_t nat_type<uint32_t>() { return H5T_NATIVE_UINT32; }
template <> hid_t nat_type<uint64_t>() { return H5T_NATIVE_UINT64; }
template <> hid_t nat_type<int32_t>() { return H5T_NATIVE_INT32; }
template <> hid_t nat_type<int64_t>() { return H5T_NATIVE_INT64; }


class reader : public FBSchemaReader {
std::unique_ptr<FBSchemaWriter> create_writer_impl() override;
std::string sourcename_impl(Msg msg) override;
uint64_t ts_impl(Msg msg) override;
};


class writer : public FBSchemaWriter {
  const hsize_t chunk_size = 1000000;
~writer() override;
  void init_impl(std::string const &, hid_t, Msg) override;
  WriteResult write_impl(Msg) override;
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
    LOG(4, "WARNING message has no source name");
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
    LOG(4, "ERROR no time data sent");
    return 0;
  }
  return ts;
}




writer::~writer() {
}

void writer::init_impl(std::string const & sourcename, hid_t hdf_group, Msg msg) {
	// TODO
	// This is just a unbuffered, low-performance write.
	// Add buffering after it works.
	LOG(6, "amo0::init_impl  v.size() == {}", chunk_size);
	grp_event = hdf_group;

	{
	  auto dt = nat_type<uint32_t>();
	  std::array<hsize_t, 1> sizes_ini {{0}};
	  std::array<hsize_t, 1> sizes_max {{H5S_UNLIMITED}};
	  this->dsp = H5Screate_simple(sizes_ini.size(), sizes_ini.data(), sizes_max.data());

	  if(true) {
	    LOG(7, "\tDataSpace isSimple {}", H5Sis_simple(dsp));
	    auto ndims = H5Sget_simple_extent_ndims(dsp);
	    LOG(7, "\tDataSpace getSimpleExtentNdims {}", ndims);
	    LOG(7, "\tDataSpace getSimpleExtentNpoints {}", 
	  	H5Sget_simple_extent_npoints(dsp));
	    std::vector<hsize_t> get_sizes_now;
	    std::vector<hsize_t> get_sizes_max;
	    get_sizes_now.resize(ndims);
	    get_sizes_max.resize(ndims);
	    H5Sget_simple_extent_dims(dsp, get_sizes_now.data(), get_sizes_max.data());
	    for (int i1 = 0; i1 < ndims; ++i1) {
	      LOG(7, "\tH5Sget_simple_extent_dims {:3} {:3}", 
	  	  get_sizes_now.at(i1), get_sizes_max.at(i1));
	    }
	  }

	  this->dpl = H5Pcreate(H5P_DATASET_CREATE);
	  std::array<hsize_t, 1> sizes_chk {{chunk_size}};
	  H5Pset_chunk(dpl, sizes_chk.size(), sizes_chk.data());
	  this->to = H5Dcreate1(hdf_group,"event_time_offset", dt, dsp, dpl);
	  this->id = H5Dcreate1(hdf_group,"event_id", dt, dsp, dpl);
	  H5Sclose(dsp);
	  H5Pclose(dpl);
	}
	{
	  auto dt = nat_type<uint64_t>();
	  std::array<hsize_t, 1> sizes_ini {{0}};
	  std::array<hsize_t, 1> sizes_max {{H5S_UNLIMITED}};
	  this->dsp = H5Screate_simple(sizes_ini.size(), sizes_ini.data(), sizes_max.data());
	  this->dpl = H5Pcreate(H5P_DATASET_CREATE);
	  std::array<hsize_t, 1> sizes_chk {{chunk_size}};
	  H5Pset_chunk(dpl, sizes_chk.size(), sizes_chk.data());
	  this->tz = H5Dcreate1(hdf_group,"event_time_zero", dt, dsp, dpl);
	  this->ei = H5Dcreate1(hdf_group,"event_index", dt, dsp, dpl);
	  H5Sclose(dsp);
	  H5Pclose(dpl);
	}

}


  std::array<hsize_t, 1> _get_size_now(const hid_t& ds) {
    using A = std::array<hsize_t, 1>;    

    auto tgt = H5Dget_space(ds);
    auto ndims = H5Sget_simple_extent_ndims(tgt);
    LOG(6, "DataSpace getSimpleExtentNdims {}", ndims);
    LOG(6, "DataSpace getSimpleExtentNpoints {}", 
	H5Sget_simple_extent_npoints(tgt));
    
    A get_sizes_now;
    A get_sizes_max;
    H5Sget_simple_extent_dims(tgt, get_sizes_now.data(), get_sizes_max.data());
    for (uint i1 = 0; i1 < get_sizes_now.size(); ++i1)
      LOG(6, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
    H5Sclose(tgt);

    return get_sizes_now;
  }


  std::array<hsize_t, 1> _h5data_extend(const hid_t& ds, std::array<hsize_t, 1> new_sizes, const std::array<hsize_t, 1>& event_size) {

    for (uint i1 = 0; i1 < new_sizes.size(); ++i1)
      new_sizes.at(i1) += event_size.at(i1);
    if (H5Dextend(ds, new_sizes.data()) < 0) {
      LOG(0, "ERROR can not extend dataset");
      return {-1ul};
    }
    return std::move(new_sizes);
  }


  template<typename value_type>
  std::array<hsize_t, 1> _h5data_write(const hid_t& ds, std::array<hsize_t, 1> sizes_now, const std::array<hsize_t, 1>& event_size, 
				       const value_type* data, const uint offset=0) {
    hid_t tgt = H5Dget_space(ds);
    herr_t err = H5Sselect_hyperslab(tgt, H5S_SELECT_SET, sizes_now.data(), nullptr, 
				     event_size.data(), nullptr);
    if (err < 0) {
      LOG(0, "ERROR can not select mem hyperslab");
      return {-1ul};
    }
    auto mem = H5Screate_simple(event_size.size(), event_size.data(), nullptr);

    auto dt = nat_type<uint32_t>();
    err = H5Dwrite(ds, dt, mem, tgt, H5P_DEFAULT, data);
    if (err < 0) {
      LOG(0, "ERROR writing failed");
      return {-1ul};
    }
    H5Sclose(mem);
    H5Sclose(tgt);
    return _get_size_now(ds);
    
  }



WriteResult writer::write_impl(Msg msg) {
  using A = std::array<hsize_t, 1>;

  auto event = EventGenerator::FlatBufs::AMOR::GetEvent(msg.data);
  if( (event->pid() != pid+1) && (pid < -1ul) ) {
    LOG(7, "amo0 stream event loss: {} -> {}", pid, event->pid());
    // TODO write into nexus log
  }

  int64_t value = event->ts();
  pid = event->pid();

  uint32_t size=event->data()->size()/2;
  hsize_t position=0;
  {
    auto & ds = this->to;
    A get_sizes_now = _get_size_now(ds);
    A new_sizes = _h5data_extend(ds, get_sizes_now, {{size}} );
    get_sizes_now = _h5data_write(ds,get_sizes_now, {{size}},event->data()->data());
    if( new_sizes != get_sizes_now )
      LOG(6,"Expected file size differs from actual size");
    position = get_sizes_now.data()[0];
  }
  {
    auto & ds = this->id;
    A get_sizes_now = _get_size_now(ds);
    A new_sizes = _h5data_extend(ds, get_sizes_now, {{size}} );
    get_sizes_now = _h5data_write(ds,get_sizes_now, {{size}},event->data()->data()+size);
    if( new_sizes != get_sizes_now )
      LOG(6,"Expected file size differs from actual size");
  }
  
  {
    auto & ds = this->tz;
    A get_sizes_now = _get_size_now(ds);
    A new_sizes = _h5data_extend(ds, get_sizes_now, {{1}});
    get_sizes_now = _h5data_write(ds,get_sizes_now, {{1}},&value);
    if( new_sizes != get_sizes_now )
      LOG(6,"Expected file size differs from actual size");
  }
  {
    auto & ds = this->ei;
    A get_sizes_now = _get_size_now(ds);
    A new_sizes = _h5data_extend(ds, get_sizes_now, {{1}});
    get_sizes_now = _h5data_write(ds,get_sizes_now, {{1}},&position);
    if( new_sizes != get_sizes_now )
      LOG(6,"Expected file size differs from actual size");
  }

  return {value};
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
