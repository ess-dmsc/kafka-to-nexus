// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "URI.h"
#include <gtest/gtest.h>

using namespace uri;

TEST(URI, host_only_gives_hostport_plus_default_port) {
  URI TestURI("//myhost");
  ASSERT_EQ(TestURI.HostPort, "myhost");
  ASSERT_EQ(TestURI.Host, "myhost");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
}

TEST(URI, host_only) {
  URI TestURI("myhost");
  ASSERT_EQ(TestURI.HostPort, "myhost");
  ASSERT_EQ(TestURI.Host, "myhost");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
}

TEST(URI, ip_only_gives_hostport_plus_default_port) {
  URI TestURI("//127.0.0.1");
  ASSERT_EQ(TestURI.HostPort, "127.0.0.1");
  ASSERT_EQ(TestURI.Host, "127.0.0.1");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
}

TEST(URI, ip_only) {
  URI TestURI("127.0.0.1");
  ASSERT_EQ(TestURI.HostPort, "127.0.0.1");
  ASSERT_EQ(TestURI.Host, "127.0.0.1");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
}

TEST(URI, host_with_port_gives_hostport_and_port) {
  URI TestURI("//myhost:345");
  ASSERT_EQ(TestURI.HostPort, "myhost:345");
  ASSERT_EQ(TestURI.Host, "myhost");
  ASSERT_EQ(TestURI.Port, (uint32_t)345);
}

TEST(URI, host_and_port) {
  URI TestURI("myhost:345");
  ASSERT_EQ(TestURI.HostPort, "myhost:345");
  ASSERT_EQ(TestURI.Host, "myhost");
  ASSERT_EQ(TestURI.Port, (uint32_t)345);
}

TEST(URI, getURIString_host_with_port_and_topic_gives_hostport_and_topic) {
  std::string TestURIString = "myhost:345/mytopic";
  URI TestURI("//" + TestURIString);
  ASSERT_EQ(TestURI.getURIString(), TestURIString);
}

TEST(URI, host_and_port_and_topic) {
  std::string TestURIString = "myhost:345/mytopic";
  URI TestURI(TestURIString);
  ASSERT_EQ(TestURI.getURIString(), TestURIString);
}

TEST(URI, ip_with_port_gives_hostport_with_port) {
  URI TestURI("//127.0.0.1:345");
  ASSERT_EQ(TestURI.HostPort, "127.0.0.1:345");
  ASSERT_EQ(TestURI.Host, "127.0.0.1");
  ASSERT_EQ(TestURI.Port, (uint32_t)345);
}

TEST(URI, ip_and_port) {
  URI TestURI("//127.0.0.1:345");
  ASSERT_EQ(TestURI.HostPort, "127.0.0.1:345");
  ASSERT_EQ(TestURI.Host, "127.0.0.1");
  ASSERT_EQ(TestURI.Port, (uint32_t)345);
}

TEST(URI, scheme_ignored_host_port_path_parsed) {
  URI TestURI("kafka://my_host.com:8080/maybe");
  ASSERT_EQ(TestURI.HostPort, "my_host.com:8080");
  ASSERT_EQ(TestURI.Host, "my_host.com");
  ASSERT_EQ(TestURI.Port, (uint32_t)8080);
  ASSERT_EQ(TestURI.Topic, "maybe");
}

TEST(URI, path_after_topic_throws_runtime_error) {
  ASSERT_THROW(URI("//my.Host:99/some/longer"), std::runtime_error);
}

TEST(URI, path_after_topic_throws_runtime_error_alt) {
  ASSERT_THROW(URI("my.Host:99/some/longer"), std::runtime_error);
}

TEST(URI, host_with_topic_no_port_gives_hostport_topic_and_default_port) {
  URI TestURI("//my.Host/the-topic");
  ASSERT_EQ(TestURI.HostPort, "my.Host");
  ASSERT_EQ(TestURI.Host, "my.Host");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
  ASSERT_EQ(TestURI.Topic, "the-topic");
}

TEST(URI, host_with_topic_and_port_gives_hostport_topic_and_port) {
  URI TestURI("//my.Host:789/the-topic");
  ASSERT_EQ(TestURI.HostPort, "my.Host:789");
  ASSERT_EQ(TestURI.Host, "my.Host");
  ASSERT_EQ(TestURI.Port, (uint32_t)789);
  ASSERT_EQ(TestURI.Topic, "the-topic");
}

TEST(URI, scheme_double_colon_throws_runtime_error) {
  ASSERT_THROW(URI("http:://my.Host"), std::runtime_error);
}

TEST(URI, no_host_given_throws_runtime_error) {
  ASSERT_THROW(URI("//:9092"), std::runtime_error);
}

TEST(URI, no_host) { ASSERT_THROW(URI(":9092"), std::runtime_error); }

TEST(URI, port_double_colon_throws_runtime_error) {
  ASSERT_THROW(URI("//my.Host::789"), std::runtime_error);
}

TEST(URI, double_colon_throws) {
  ASSERT_THROW(URI("my.Host::789"), std::runtime_error);
}

TEST(URI, host_and_port_with_spaces_still_parsed_correctly) {
  URI TestURI("  //some:123     ");
  ASSERT_EQ(TestURI.HostPort, "some:123");
  ASSERT_EQ(TestURI.Host, "some");
  ASSERT_EQ(TestURI.Port, 123u);
}

TEST(URI, topic_picked_up_when_after_port) {
  URI TestURI("//localhost:9092/TEST_writerCommand");
  ASSERT_EQ(TestURI.HostPort, "localhost:9092");
  ASSERT_EQ(TestURI.Host, "localhost");
  ASSERT_EQ(TestURI.Port, 9092u);
  ASSERT_EQ(TestURI.Topic, "TEST_writerCommand");
  ASSERT_EQ(TestURI.getURIString(), "localhost:9092/TEST_writerCommand");
}

TEST(URI, EmptyStringDoesNotThrow) {
  try {
    URI TestURI("");
    EXPECT_TRUE(TestURI.HostPort.empty());
  } catch (...) {
    FAIL();
  }
}
