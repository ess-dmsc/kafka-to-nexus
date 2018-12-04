#include "URI.h"
#include <gtest/gtest.h>

using namespace uri;

TEST(URI, host) {
  URI TestURI("//myhost");
  ASSERT_EQ(TestURI.Host, "myhost");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
}

TEST(URI, ip) {
  URI TestURI("//127.0.0.1");
  ASSERT_EQ(TestURI.Host, "127.0.0.1");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
}

TEST(URI, host_port) {
  URI TestURI("//myhost:345");
  ASSERT_EQ(TestURI.Host, "myhost");
  ASSERT_EQ(TestURI.Port, (uint32_t)345);
}

TEST(URI, expectedURIStringWhenHostPortAndTopicAreSpecified) {
  std::string TestURIString = "//myhost:345/mytopic";
  URI TestURI(TestURIString);
  ASSERT_EQ(TestURI.getURIString(), TestURIString);
}

TEST(URI, ip_port) {
  URI TestURI("//127.0.0.1:345");
  ASSERT_EQ(TestURI.Host, "127.0.0.1");
  ASSERT_EQ(TestURI.Port, (uint32_t)345);
}

TEST(URI, scheme_ignored_host_port_path_parsed) {
  URI TestURI("kafka://my_host.com:8080/maybe");
  ASSERT_EQ(TestURI.Host, "my_host.com");
  ASSERT_EQ(TestURI.Port, (uint32_t)8080);
  ASSERT_EQ(TestURI.Path, "/maybe");
}

TEST(URI, topic_after_path_parsed) {
  URI TestURI("//my.Host:99/some/longer");
  ASSERT_EQ(TestURI.Host, "my.Host");
  ASSERT_EQ(TestURI.Port, (uint32_t)99);
  ASSERT_EQ(TestURI.Path, "/some/longer");
  ASSERT_EQ(TestURI.Topic, "longer");
}

TEST(URI, host_topic) {
  URI TestURI("//my.Host/the-topic");
  ASSERT_EQ(TestURI.Host, "my.Host");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
  ASSERT_EQ(TestURI.Topic, "the-topic");
}

TEST(URI, host_port_topic) {
  URI TestURI("//my.Host:789/the-topic");
  ASSERT_EQ(TestURI.Host, "my.Host");
  ASSERT_EQ(TestURI.Port, (uint32_t)789);
  ASSERT_EQ(TestURI.Topic, "the-topic");
}

TEST(URI, scheme_double_colon_ignored) {
  URI TestURI("http:://my.Host");
  ASSERT_EQ(TestURI.Host, "my.Host");
}

TEST(URI, port_double_colon_ignored) {
  URI TestURI("//my.Host::789");
  ASSERT_EQ(TestURI.Host, "my.Host");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
}

TEST(URI, trim) {
  URI TestURI("  //some:123     ");
  ASSERT_EQ(TestURI.Host, "some");
  ASSERT_EQ(TestURI.Port, 123u);
}
