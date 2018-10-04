#include <StreamMaster.h>
#include <gtest/gtest.h>

#include <iostream>

// class StubStreamer : public FileWriter::IStreamer {
// public:
//   using Error = FileWriter::Status::StreamerStatus;
//   using Options = std::vector<std::string>;
//
//   StubStreamer() {}
//   StubStreamer(const std::string &, const std::string &, const Options &,
//                const Options &) {}
//   const Error set_start_time(const std::chrono::milliseconds &) const {
//     return Error::OK;
//   }
//   int &n_sources() { return n_src; };
//
// private:
//   int n_src;
// };
//
// using StreamMaster = FileWriter::StreamMaster;
//
// TEST(StreamMaster, empty_test) {
//   auto fwt =
//       std::make_unique<FileWriter::FileWriterTask>("service-id", nullptr);
//
//   fwt->demuxers().push_back(FileWriter::DemuxTopic("dummy-topic"));
//
//   StreamMaster Master("dummy-broker", std::move(fwt), MainOpt());
// }
