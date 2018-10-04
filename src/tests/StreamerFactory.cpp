#include "../StreamerFactory.h"
#include "../DemuxTopic.h"
#include "../StreamerI.h"
#include "../StreamerOptions.h"

#include <gtest/gtest.h>
#include <iostream>
#include <typeinfo>

using namespace FileWriter;

class StubStreamer1 : public IStreamer {
  std::string getName() const override { return "stub1"; }

private:
  ProcessMessageResult pollAndProcess(FileWriter::DemuxTopic &) override {
    return ProcessMessageResult::OK;
  }
};

class StubStreamer2 : public IStreamer {
  std::string getName() const override { return "stub2"; }

private:
  ProcessMessageResult pollAndProcess(FileWriter::DemuxTopic &) override {
    return ProcessMessageResult::OK;
  }
};

template <class Stub> class StubFactory : public IStreamerFactory {
  using Object = Stub;

public:
  std::unique_ptr<IStreamer> create(const std::string &, DemuxTopic &,
                                    const StreamerOptions &) {
    return std::make_unique<Object>();
  }
};

TEST(StreamerFactory, CreateDummyObject) {
  StubFactory<StubStreamer1> Factory1;
  DemuxTopic Demux("dummy-topic");
  StreamerOptions Opt;
  auto Stub1 = Factory1.create("dummy-broker", Demux, Opt);
  EXPECT_TRUE(Stub1);
}

TEST(StreamerFactory, CreateDifferentObjects) {
  StubFactory<StubStreamer1> Factory1;
  StubFactory<StubStreamer2> Factory2;
  DemuxTopic Demux("dummy-topic");
  StreamerOptions Opt;
  auto Stub1 = Factory1.create("dummy-broker", Demux, Opt);
  auto Stub2 = Factory2.create("dummy-broker", Demux, Opt);
  EXPECT_NE(Stub1->getName(), Stub2->getName());
}
