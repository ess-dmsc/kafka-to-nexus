// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "mdat_Writer.h"
#include "WriterRegistrar.h"
#include "HDFOperations.h"

namespace WriterModule::mdat {

// Register our module so the parsing of JSON recognises it.
static WriterModule::Registry::Registrar<mdat_Writer> RegisterWriter("mdat",
                                                                     "mdat");

WriterModule::InitResult
mdat_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
  try {
    NeXusDataset::DateTime(HDFGroup, "start_time", NeXusDataset::Mode::Create, StringSize,
                           ChunkSize);
    NeXusDataset::DateTime(HDFGroup, "end_time", NeXusDataset::Mode::Create, StringSize,
                           ChunkSize);
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    LOG_ERROR("mdat could not init_hdf HDFGroup: {}  trace: {}\nError with "
              "NeXusDataset::Time(..)?",
              static_cast<std::string>(HDFGroup.link().path()), message);
    return WriterModule::InitResult::ERROR;
  }

  return WriterModule::InitResult::OK;
}

WriterModule::InitResult mdat_Writer::reopen(hdf5::node::Group &HDFGroup) {
  try {
    mdatStart_datetime = NeXusDataset::DateTime(
        HDFGroup, "start_time", NeXusDataset::Mode::Open, StringSize, ChunkSize);
    mdatEnd_datetime = NeXusDataset::DateTime(
        HDFGroup, "end_time", NeXusDataset::Mode::Open, StringSize, ChunkSize);
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    LOG_ERROR("mdat could not reopen HDFGroup: {}  trace: {}\nError with "
              "NeXusDataset::DateTime(..)?",
              static_cast<std::string>(HDFGroup.link().path()), message);
    return WriterModule::InitResult::ERROR;
  }

  return WriterModule::InitResult::OK;
}

template <typename T>
void mdat_Writer::writemetadata(std::string const &name,
                                T data) { //  all is valid
  char buffer[32];
  time_t datatime = std::chrono::system_clock::to_time_t(data);
  tm *nowtm = gmtime(&datatime);
  std::strftime(buffer, 32, "%FT%TZ%z", nowtm);
//  std::cout << "mdatmdat" << buffer << "," << datatime <<std::endl <<std::endl <<std::endl;
  //   auto DataType = hdf5::datatype::String::fixed(32);
  //   DataType.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  //   DataType.padding(hdf5::datatype::StringPad::NullTerm);
  // //   auto StringArray = HDFOperations::jsonArrayToMultiArray<std::string>("A");
  // // auto Dims = StringArray.getDimensions();

  // //   auto Dataspace =
  // //       hdf5::dataspace::Simple(hdf5::Dimensions(Dims.begin(), Dims.end()));
  // std::string strBuffer = std::string(buffer);
  // auto StringArray = HDFOperations::jsonArrayToMultiArray<std::string>(strBuffer);

  if (name == "start_time")
//    mdatStart_datetime.write(StringArray.Data, DataType, Dataspace);
    mdatStart_datetime.appendStringElement(std::string(buffer));

    //     auto DataType = hdf5::datatype::String::variable();
    // DataType.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    // DataType.padding(hdf5::datatype::StringPad::NullTerm);
    // auto StringArray = jsonArrayToMultiArray<std::string>(Values);
    // auto Dims = StringArray.getDimensions();

    // auto Dataspace =
    //     hdf5::dataspace::Simple(hdf5::Dimensions(Dims.begin(), Dims.end()));

    // Parent.create_dataset(Name, DataType, Dataspace)
    //     .write(StringArray.Data, DataType, Dataspace);


//    mdatStart_datetime.appendArray(hdf5::ArrayAdapter<char>(buffer, 32));
//  else if (name == "end_time")
//    mdatEnd_datetime.appendArray(hdf5::ArrayAdapter<const char[]>(&buffer, 32));
}

//  avoid linker errors by instantiating a version of the template with expected
//  data types
template void mdat_Writer::writemetadata(std::string const &name,
                                         time_point data);

} // namespace WriterModule::mdat
