// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Stream/Topic.h"
#include "KafkaW/ConsumerFactory.h"
#include "KafkaW/Consumer.h"
#include "Metrics/Registrar.h"
#include <gtest/gtest.h>
#include <memory>
#include "Stream/Partition.h"

using std::chrono_literals::operator""s;

class TopicStandIn : public Stream::Topic {
public:
TopicStandIn(KafkaW::BrokerSettings Settings, std::string Topic, Stream::SrcToDst Map,
Stream::MessageWriter *Writer, Metrics::Registrar &RegisterMetric,
Stream::time_point StartTime, Stream::duration StartTimeLeeway,
Stream::time_point StopTime, Stream::duration StopTimeLeeway) : Stream::Topic(Settings,Topic, Map, Writer, RegisterMetric, StartTime, StartTimeLeeway, StopTime, StopTimeLeeway) {}
};

class TopicTest : public ::testing::Test {
public:
  void SetUp() override {
    Stream::time_point Start(std::chrono::system_clock::now());
    Stream::time_point Stop(std::chrono::system_clock::time_point::max());
    Metrics::Registrar Registrar("some_name", {});
    Stream::SrcToDst Map;
    UnderTest = std::make_unique<Stream::Topic>(KafkaSettings, "some_topic", Map, nullptr, Registrar, Start, 5s, Stop, 5s);
  }
  KafkaW::BrokerSettings KafkaSettings;
  std::unique_ptr<Stream::Topic> UnderTest;
};


