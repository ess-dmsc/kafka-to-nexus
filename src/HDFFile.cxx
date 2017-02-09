#include "HDFFile.h"
#include "HDFFile_h5.h"
#include <array>
#include <hdf5.h>
#include "schemas/f140_general_generated.h"
//#include "f141-ntarraydouble_generated.h"
#include "schemas/rit0_psi_sinq_schema_generated.h"
#include "logger.h"


namespace BrightnESS {
  namespace FileWriter {

    template <typename T> hid_t nat_type();
    template <> hid_t nat_type<float>()    { return H5T_NATIVE_FLOAT; }
    template <> hid_t nat_type<double>()   { return H5T_NATIVE_DOUBLE; }
    template <> hid_t nat_type<uint32_t>() { return H5T_NATIVE_UINT32; }
    template <> hid_t nat_type<uint64_t>() { return H5T_NATIVE_UINT64; }



    class HDFFile_impl {
      friend class HDFFile;
      hid_t h5file = -1;
    };

    HDFFile::HDFFile() {
      impl.reset(new HDFFile_impl);
    }

    HDFFile::~HDFFile() {
      if (impl->h5file >= 0) {
        H5Fclose(impl->h5file);
      }
    }

    void HDFFile::init(std::string filename) {
      impl->h5file = H5Fcreate(filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    }

    void HDFFile::flush() {
      H5Fflush(impl->h5file, H5F_SCOPE_LOCAL);
    }

    HDFFile_h5 HDFFile::h5file_detail() {
      return HDFFile_h5(impl->h5file);
    }

    HDFFile_h5::HDFFile_h5(hid_t h5file) : _h5file(h5file) {
    }

    hid_t HDFFile_h5::h5file() {
      return _h5file;
    }


    namespace schemareaders {

      class amor_event : public FBSchemaReader {
      public:
        amor_event();
        std::unique_ptr<FBSchemaWriter> create_writer_impl();
        std::string sourcename_impl(char * msg_data);
        uint64_t ts_impl(char * msg_data);
      };
    }

    namespace schemawriters {

      class amor_event : public FBSchemaWriter {
      public:
        amor_event();
        ~amor_event();
        void init_impl(std::string const & sourcename, char * msg);
        WriteResult write_impl(char * msg_data);
      private:
        // DataSet::getId() will be -1 for the default constructed
        hid_t pid = -1;
      };
    }

    namespace schemareaders {

      amor_event::amor_event() {
      }
      std::unique_ptr<FBSchemaWriter> amor_event::create_writer_impl() {
	return std::unique_ptr<FBSchemaWriter>(new schemawriters::amor_event);
      }
      std::string amor_event::sourcename_impl(char * msg_data) {
	auto event = BrightnESS::EventGenerator::FlatBufs::AMOR::GetEvent(msg_data);
	return event->htype()->str();
      }
      uint64_t amor_event::ts_impl(char * msg_data) {
	auto event = BrightnESS::EventGenerator::FlatBufs::AMOR::GetEvent(msg_data);
	return event->ts();
      }
    }

    namespace schemawriters {

      amor_event::amor_event() {
      }
      amor_event::~amor_event() {
	// TODO
	// H5Dclose(ds);
	// H5Sclose(dsp);
	// H5Pclose(dcpl);
      }
      void amor_event::init_impl(std::string const & sourcename, char * msg_data) {
	// TODO
	// This is just a unbuffered, low-performance write.
	// Add buffering after it works.
	auto file = hdf_file->h5file_detail().h5file();
	auto event = BrightnESS::EventGenerator::FlatBufs::AMOR::GetEvent(msg_data);
	auto nev = event->data()->size();
	LOG(1, "amor_event::init_impl  data.size() == {}", nev);
	// using std::vector;
	// using std::array;
	// auto dt = nat_type<uint64_t>();
	// // H5S_UNLIMITED
	// std::array<hsize_t, 2> sizes_ini {{0, nev}};
	// std::array<hsize_t, 2> sizes_max {{H5S_UNLIMITED, H5S_UNLIMITED}};

	// // TODO
	// // Lifetime?  Must it live as long as dataset exists?
	// this->dsp = H5Screate_simple(sizes_ini.size(), sizes_ini.data(), sizes_max.data());
	// if (true) {
        //   // Just check if it works as I think it should
        //   LOG(0, "DataSpace isSimple {}", H5Sis_simple(dsp));
        //   auto ndims = H5Sget_simple_extent_ndims(dsp);
        //   LOG(0, "DataSpace getSimpleExtentNdims {}", ndims);
        //   LOG(0, "DataSpace getSimpleExtentNpoints {}", H5Sget_simple_extent_npoints(dsp));
        //   std::vector<hsize_t> get_sizes_now;
        //   std::vector<hsize_t> get_sizes_max;
        //   get_sizes_now.resize(ndims);
        //   get_sizes_max.resize(ndims);
        //   H5Sget_simple_extent_dims(dsp, get_sizes_now.data(), get_sizes_max.data());
        //   for (int i1 = 0; i1 < ndims; ++i1) {
        //     LOG(0, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
        //   }
	// }

	// // TODO
	// // Lifetime of this guy?
	// // Together with the dataset?
	// this->dcpl = H5Pcreate(H5P_DATASET_CREATE);
	// std::array<hsize_t, 2> sizes_chk {{std::max(64*1024/H5Tget_size(dt)/ncols, (size_t)1), ncols}};
	// H5Pset_chunk(dcpl, sizes_chk.size(), sizes_chk.data());
	// this->ds = H5Dcreate1(file, sourcename.c_str(), dt, dsp, dcpl);
      }

      WriteResult amor_event::write_impl(char * msg_data) {
	LOG(0, "amor_event::write_impl");
	// TODO cache these values later, but for now, let's make it verbose:

	// // Just being explicit here
	// auto & ds = this->ds;
	// auto tgt = H5Dget_space(ds);
	// auto ndims = H5Sget_simple_extent_ndims(tgt);
	// std::vector<hsize_t> get_sizes_now;
	// std::vector<hsize_t> get_sizes_max;
	// get_sizes_now.resize(ndims);
	// get_sizes_max.resize(ndims);
	// H5Sget_simple_extent_dims(tgt, get_sizes_now.data(), get_sizes_max.data());
	// for (int i1 = 0; i1 < ndims; ++i1) {
        //   LOG(0, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
	// }

        auto event = BrightnESS::EventGenerator::FlatBufs::AMOR::GetEvent(msg_data);
	// if (event->data()->size() != get_sizes_now.at(1)) {
        //   LOG(7, "ERROR this message is not compatible with the previous ones");
        //   return {-1};
	// }

	// get_sizes_now.at(0) += 1;
	// herr_t err;
	// err = H5Dextend(ds, get_sizes_now.data());
	// if (err < 0) {
        //   LOG(7, "ERROR can not extend dataset");
        //   return {-1};
	// }
	// H5Sclose(tgt);

	// tgt = H5Dget_space(ds);
	// using A = std::array<hsize_t, 2>;
	// A mem_size = {{1, get_sizes_now.at(1)}};
	// auto mem = H5Screate_simple(2, mem_size.data(), nullptr);
	// {
        //   A hsl_start {{0, 0}};
        //   A hsl_count {{1, get_sizes_now.at(1)}};
        //   err = H5Sselect_hyperslab(mem, H5S_SELECT_SET, hsl_start.data(), nullptr, hsl_count.data(), nullptr);
        //   if (err < 0) {
        //     LOG(7, "ERROR can not select mem hyperslab");
        //     return {-1};
        //   }
	// }
	// {
        //   A hsl_start {{get_sizes_now.at(0)-1, 0}};
        //   A hsl_count {{1, get_sizes_now.at(1)}};
        //   err = H5Sselect_hyperslab(tgt, H5S_SELECT_SET, hsl_start.data(), nullptr, hsl_count.data(), nullptr);
        //   if (err < 0) {
        //     LOG(7, "ERROR can not select tgt hyperslab");
        //     return {-1};
        //   }
	// }
	// for (int i1 = 0; i1 < ndims; ++i1) {
        //   LOG(0, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
	// }
	// auto dt = nat_type<DT>();
	// err = H5Dwrite(ds, dt, mem, tgt, H5P_DEFAULT, pv->v()->data());
	// if (err < 0) {
        //   LOG(7, "ERROR writing failed");
        //   return {-1};
	// }
	// H5Sclose(mem);
	return {static_cast<int64_t>(event->ts())};
      }

      
    } //namespace schemawriters


    std::unique_ptr<FBSchemaReader> FBSchemaReader::create(char * msg_data, int msg_size) {
      static_assert(FLATBUFFERS_LITTLEENDIAN, "Requires currently little endian");
      auto sid = *((uint16_t*)msg_data);
      LOG(0, "sid: {:x}", sid);
      if (sid == 0xf140) {
        return nullptr;
        //return std::unique_ptr<FBSchemaReader>(new schemareaders::f140_general);
      }
      // if (sid == 0xf141) {
      //   return std::unique_ptr<FBSchemaReader>(new schemareaders::f141_ntarraydouble);
      // }
      if( BrightnESS::EventGenerator::FlatBufs::AMOR::EventBufferHasIdentifier(reinterpret_cast<const void*>(msg_data)) ) {
        return std::unique_ptr<FBSchemaReader>(new schemareaders::amor_event);
      }
      
      return nullptr;
    }

    std::unique_ptr<FBSchemaWriter> FBSchemaReader::create_writer() {
      return create_writer_impl();
    }

    std::string FBSchemaReader::sourcename(char * msg_data) {
      return sourcename_impl(msg_data);
    }

    uint64_t FBSchemaReader::ts(char * msg_data) {
      return ts_impl(msg_data);
    }


    FBSchemaWriter::FBSchemaWriter() {
    }

    FBSchemaWriter::~FBSchemaWriter() {
    }

    void FBSchemaWriter::init(HDFFile * hdf_file, std::string const & sourcename, char * msg_data) {
      this->hdf_file = hdf_file;
      init_impl(sourcename, msg_data);
    }

    WriteResult FBSchemaWriter::write(char * msg_data) {
      return write_impl(msg_data);
    }



  }
}
