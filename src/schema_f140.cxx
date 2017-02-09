namespace BrightnESS {
namespace FileWriter {
namespace schemareaders {

      f140_general::f140_general() {
      }
      std::unique_ptr<FBSchemaWriter> f140_general::create_writer_impl() {
	return std::unique_ptr<FBSchemaWriter>(new schemawriters::f140_general);
      }
      std::string f140_general::sourcename_impl(char * msg_data) {
	auto pv = BrightnESS::ForwardEpicsToKafka::FlatBufs::f140_general::GetPV(msg_data + 2);
	return pv->src()->str();
      }
      uint64_t f140_general::ts_impl(char * msg_data) {
	auto pv = BrightnESS::ForwardEpicsToKafka::FlatBufs::f140_general::GetPV(msg_data + 2);
	return pv->ts();
      }

      // f141_ntarraydouble::f141_ntarraydouble() {
      // }
      // std::unique_ptr<FBSchemaWriter> f141_ntarraydouble::create_writer_impl() {
      //   return std::unique_ptr<FBSchemaWriter>(new schemawriters::f141_ntarraydouble);
      // }
      // std::string f141_ntarraydouble::sourcename_impl(char * msg_data) {
      //   auto pv = BrightnESS::ForwardEpicsToKafka::FlatBufs::f141_ntarraydouble::GetPV(msg_data + 2);
      //   return pv->src()->str();
      // }
      // uint64_t f141_ntarraydouble::ts_impl(char * msg_data) {
      //   auto pv = BrightnESS::ForwardEpicsToKafka::FlatBufs::f141_ntarraydouble::GetPV(msg_data + 2);
      //   return pv->ts();
      // }



      class f140_general : public FBSchemaReader {
      public:
        f140_general();
        std::unique_ptr<FBSchemaWriter> create_writer_impl();
        std::string sourcename_impl(char * msg_data);
        uint64_t ts_impl(char * msg_data);
      };

      // class f141_ntarraydouble : public FBSchemaReader {
      // public:
      //   f141_ntarraydouble();
      //   std::unique_ptr<FBSchemaWriter> create_writer_impl();
      //   std::string sourcename_impl(char * msg_data);
      //   uint64_t ts_impl(char * msg_data);
      // };





namespace schemawriters {

      class f140_general : public FBSchemaWriter {
      public:
        f140_general();
        void init_impl(std::string const & sourcename, char * msg);
        WriteResult write_impl(char * msg_data);
      private:
        H5::DataSet ds;
      };

      // class f141_ntarraydouble : public FBSchemaWriter {
      // public:
      //   f141_ntarraydouble();
      //   ~f141_ntarraydouble();
      //   void init_impl(std::string const & sourcename, char * msg);
      //   WriteResult write_impl(char * msg_data);
      // private:
      //   using DT = double;
      //   // DataSet::getId() will be -1 for the default constructed
      //   hid_t ds = -1;
      //   hid_t dsp = -1;
      //   hid_t dcpl = -1;
      // };







      f140_general::f140_general() {
      }
      void f140_general::init_impl(std::string const & sourcename, char * msg_data) {
	LOG(3, "f140_general init");
      }
      WriteResult f140_general::write_impl(char * msg_data) {
	return {-1};
      }

      // f141_ntarraydouble::f141_ntarraydouble() {
      // }
      // f141_ntarraydouble::~f141_ntarraydouble() {
      //   // TODO
      //   // Check dtor order w.r.t. to the file
      //   H5Dclose(ds);
      //   H5Sclose(dsp);
      //   H5Pclose(dcpl);
      // }
      // void f141_ntarraydouble::init_impl(std::string const & sourcename, char * msg_data) {
      //   // TODO
      //   // This is just a unbuffered, low-performance write.
      //   // Add buffering after it works.
      //   auto file = hdf_file->h5file_detail().h5file();
      //   auto pv = BrightnESS::ForwardEpicsToKafka::FlatBufs::f141_ntarraydouble::GetPV(msg_data + 2);
      //   auto ncols = pv->v()->size();
      //   LOG(1, "f141_ntarraydouble::init_impl  v.size() == {}", ncols);
      //   using std::vector;
      //   using std::array;
      //   auto dt = nat_type<DT>();
      //   // H5S_UNLIMITED
      //   std::array<hsize_t, 2> sizes_ini {{0, ncols}};
      //   std::array<hsize_t, 2> sizes_max {{H5S_UNLIMITED, H5S_UNLIMITED}};

      //   // TODO
      //   // Lifetime?  Must it live as long as dataset exists?
      //   this->dsp = H5Screate_simple(sizes_ini.size(), sizes_ini.data(), sizes_max.data());
      //   if (true) {
      //     // Just check if it works as I think it should
      //     LOG(0, "DataSpace isSimple {}", H5Sis_simple(dsp));
      //     auto ndims = H5Sget_simple_extent_ndims(dsp);
      //     LOG(0, "DataSpace getSimpleExtentNdims {}", ndims);
      //     LOG(0, "DataSpace getSimpleExtentNpoints {}", H5Sget_simple_extent_npoints(dsp));
      //     std::vector<hsize_t> get_sizes_now;
      //     std::vector<hsize_t> get_sizes_max;
      //     get_sizes_now.resize(ndims);
      //     get_sizes_max.resize(ndims);
      //     H5Sget_simple_extent_dims(dsp, get_sizes_now.data(), get_sizes_max.data());
      //     for (int i1 = 0; i1 < ndims; ++i1) {
      //       LOG(0, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
      //     }
      //   }

      //   // TODO
      //   // Lifetime of this guy?
      //   // Together with the dataset?
      //   this->dcpl = H5Pcreate(H5P_DATASET_CREATE);
      //   std::array<hsize_t, 2> sizes_chk {{std::max(64*1024/H5Tget_size(dt)/ncols, (size_t)1), ncols}};
      //   H5Pset_chunk(dcpl, sizes_chk.size(), sizes_chk.data());
      //   this->ds = H5Dcreate1(file, sourcename.c_str(), dt, dsp, dcpl);
      // }

      // WriteResult f141_ntarraydouble::write_impl(char * msg_data) {
      //   LOG(0, "f141_ntarraydouble::write_impl");
      //   // TODO cache these values later, but for now, let's make it verbose:

      //   // Just being explicit here
      //   auto & ds = this->ds;
      //   auto tgt = H5Dget_space(ds);
      //   auto ndims = H5Sget_simple_extent_ndims(tgt);
      //   std::vector<hsize_t> get_sizes_now;
      //   std::vector<hsize_t> get_sizes_max;
      //   get_sizes_now.resize(ndims);
      //   get_sizes_max.resize(ndims);
      //   H5Sget_simple_extent_dims(tgt, get_sizes_now.data(), get_sizes_max.data());
      //   for (int i1 = 0; i1 < ndims; ++i1) {
      //     LOG(0, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
      //   }

      //   auto pv = BrightnESS::ForwardEpicsToKafka::FlatBufs::f141_ntarraydouble::GetPV(msg_data + 2);
      //   if (pv->v()->size() != get_sizes_now.at(1)) {
      //     LOG(7, "ERROR this message is not compatible with the previous ones");
      //     return {-1};
      //   }

      //   get_sizes_now.at(0) += 1;
      //   herr_t err;
      //   err = H5Dextend(ds, get_sizes_now.data());
      //   if (err < 0) {
      //     LOG(7, "ERROR can not extend dataset");
      //     return {-1};
      //   }
      //   H5Sclose(tgt);

      //   tgt = H5Dget_space(ds);
      //   using A = std::array<hsize_t, 2>;
      //   A mem_size = {{1, get_sizes_now.at(1)}};
      //   auto mem = H5Screate_simple(2, mem_size.data(), nullptr);
      //   {
      //     A hsl_start {{0, 0}};
      //     A hsl_count {{1, get_sizes_now.at(1)}};
      //     err = H5Sselect_hyperslab(mem, H5S_SELECT_SET, hsl_start.data(), nullptr, hsl_count.data(), nullptr);
      //     if (err < 0) {
      //       LOG(7, "ERROR can not select mem hyperslab");
      //       return {-1};
      //     }
      //   }
      //   {
      //     A hsl_start {{get_sizes_now.at(0)-1, 0}};
      //     A hsl_count {{1, get_sizes_now.at(1)}};
      //     err = H5Sselect_hyperslab(tgt, H5S_SELECT_SET, hsl_start.data(), nullptr, hsl_count.data(), nullptr);
      //     if (err < 0) {
      //       LOG(7, "ERROR can not select tgt hyperslab");
      //       return {-1};
      //     }
      //   }
      //   for (int i1 = 0; i1 < ndims; ++i1) {
      //     LOG(0, "H5Sget_simple_extent_dims {:3} {:3}", get_sizes_now.at(i1), get_sizes_max.at(i1));
      //   }
      //   auto dt = nat_type<DT>();
      //   err = H5Dwrite(ds, dt, mem, tgt, H5P_DEFAULT, pv->v()->data());
      //   if (err < 0) {
      //     LOG(7, "ERROR writing failed");
      //     return {-1};
      //   }
      //   H5Sclose(mem);
      //   return {pv->ts()};
      // }




