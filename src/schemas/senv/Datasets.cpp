#include "Datasets.h"

namespace NeXusDataset {
  RawValue::RawValue(hdf5::node::Group parent, int ChunkSize) : hdf5::node::ChunkedDataset(parent, "raw_value", hdf5::datatype::create<std::uint16_t>(), hdf5::dataspace::Simple({0}, {hdf5::dataspace::Simple::UNLIMITED}), {static_cast<unsigned long long>(ChunkSize), }) {
    
  }
  
  RawValue::RawValue(hdf5::node::Group parent) : hdf5::node::ChunkedDataset() {
    Dataset::operator=(parent.get_dataset("raw_value"));
  }
  
  Time::Time(hdf5::node::Group parent, int ChunkSize) : hdf5::node::ChunkedDataset(parent, "time", hdf5::datatype::create<std::uint64_t>(), hdf5::dataspace::Simple({0}, {hdf5::dataspace::Simple::UNLIMITED}), {static_cast<unsigned long long>(ChunkSize), }) {
    
  }
  
  Time::Time(hdf5::node::Group parent) : hdf5::node::ChunkedDataset() {
    Dataset::operator=(parent.get_dataset("time"));
  }
  
  CueIndex::CueIndex(hdf5::node::Group parent, int ChunkSize) : hdf5::node::ChunkedDataset(parent, "cue_index", hdf5::datatype::create<std::uint32_t>(), hdf5::dataspace::Simple({0}, {hdf5::dataspace::Simple::UNLIMITED}), {static_cast<unsigned long long>(ChunkSize), }) {
    
  }
  
  CueIndex::CueIndex(hdf5::node::Group parent) : hdf5::node::ChunkedDataset() {
    Dataset::operator=(parent.get_dataset("cue_index"));
  }
  
  CueTimestampZero::CueTimestampZero(hdf5::node::Group parent, int ChunkSize) : hdf5::node::ChunkedDataset(parent, "cue_timestamp_zero", hdf5::datatype::create<std::uint64_t>(), hdf5::dataspace::Simple({0}, {hdf5::dataspace::Simple::UNLIMITED}), {static_cast<unsigned long long>(ChunkSize), }) {
    
  }
  
  CueTimestampZero::CueTimestampZero(hdf5::node::Group parent) : hdf5::node::ChunkedDataset() {
    Dataset::operator=(parent.get_dataset("cue_timestamp_zero"));
  }
  
  
} // namespace NeXusDataset
