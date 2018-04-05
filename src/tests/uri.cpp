#include "../uri.h"
#include <gtest/gtest.h>

using namespace uri;

TEST(URI, host) {
  URI TestURI("//myhost");
  ASSERT_EQ(TestURI.scheme, "");
  ASSERT_EQ(TestURI.host, "myhost");
  ASSERT_EQ(TestURI.port, (uint32_t)0);
}

TEST(URI, ip) {
  URI TestURI("//127.0.0.1");
  ASSERT_EQ(TestURI.scheme, "");
  ASSERT_EQ(TestURI.host, "127.0.0.1");
  ASSERT_EQ(TestURI.port, (uint32_t)0);
}

TEST(URI, host_port) {
  URI TestURI("//myhost:345");
  ASSERT_EQ(TestURI.scheme, "");
  ASSERT_EQ(TestURI.host, "myhost");
  ASSERT_EQ(TestURI.port, (uint32_t)345);
}

TEST(URI, expectedURIStringWhenHostPortAndTopicAreSpecified) {
  std::string TestURIString = "//myhost:345/mytopic";
  URI TestURI(TestURIString);
  ASSERT_EQ(TestURI.getURIString(), TestURIString);
}

TEST(URI, host_port_noslashes) {
  URI TestURI;
  TestURI.require_host_slashes = false;
  TestURI.parse("myhost:345");
  ASSERT_EQ(TestURI.scheme, "");
  ASSERT_EQ(TestURI.host, "myhost");
  ASSERT_EQ(TestURI.port, (uint32_t)345);
}

TEST(URI, ip_port) {
  URI TestURI("//127.0.0.1:345");
  ASSERT_EQ(TestURI.scheme, "");
  ASSERT_EQ(TestURI.host, "127.0.0.1");
  ASSERT_EQ(TestURI.port, (uint32_t)345);
}

TEST(URI, scheme_host_port) {
  URI TestURI;
  TestURI.port = 123;
  TestURI.parse("http://my.host:345");
  ASSERT_EQ(TestURI.scheme, "http");
  ASSERT_EQ(TestURI.host, "my.host");
  ASSERT_EQ(TestURI.host_port, "my.host:345");
  ASSERT_EQ(TestURI.port, (uint32_t)345);
}

TEST(URI, scheme_host_port_default) {
  URI TestURI;
  TestURI.port = 123;
  TestURI.parse("http://my.host");
  ASSERT_EQ(TestURI.scheme, "http");
  ASSERT_EQ(TestURI.host, "my.host");
  ASSERT_EQ(TestURI.host_port, "my.host:123");
  ASSERT_EQ(TestURI.port, (uint32_t)123);
}

TEST(URI, scheme_host_port_pathdefault) {
  URI TestURI("kafka://my-host.com:8080/");
  ASSERT_EQ(TestURI.scheme, "kafka");
  ASSERT_EQ(TestURI.host, "my-host.com");
  ASSERT_EQ(TestURI.port, (uint32_t)8080);
  ASSERT_EQ(TestURI.path, "/");
}

TEST(URI, scheme_host_port_path) {
  URI TestURI("kafka://my-host.com:8080/som_e");
  ASSERT_EQ(TestURI.scheme, "kafka");
  ASSERT_EQ(TestURI.host, "my-host.com");
  ASSERT_EQ(TestURI.port, (uint32_t)8080);
  ASSERT_EQ(TestURI.path, "/som_e");
  ASSERT_EQ(TestURI.topic, "som_e");
}

TEST(URI, scheme_host_port_pathlonger) {
  URI TestURI("kafka://my_host.com:8080/some/longer");
  ASSERT_EQ(TestURI.scheme, "kafka");
  ASSERT_EQ(TestURI.host, "my_host.com");
  ASSERT_EQ(TestURI.port, (uint32_t)8080);
  ASSERT_EQ(TestURI.path, "/some/longer");
  ASSERT_EQ(TestURI.topic, "");
}

TEST(URI, host_topic) {
  URI TestURI("//my.host/the-topic");
  ASSERT_EQ(TestURI.scheme, "");
  ASSERT_EQ(TestURI.host, "my.host");
  ASSERT_EQ(TestURI.port, (uint32_t)0);
  ASSERT_EQ(TestURI.topic, "the-topic");
}

TEST(URI, host_port_topic) {
  URI TestURI("//my.host:789/the-topic");
  ASSERT_EQ(TestURI.scheme, "");
  ASSERT_EQ(TestURI.host, "my.host");
  ASSERT_EQ(TestURI.port, (uint32_t)789);
  ASSERT_EQ(TestURI.topic, "the-topic");
}

TEST(URI, abspath) {
  URI TestURI("/mypath/sub");
  ASSERT_EQ(TestURI.scheme, "");
  ASSERT_EQ(TestURI.host, "");
  ASSERT_EQ(TestURI.port, (uint32_t)0);
  ASSERT_EQ(TestURI.path, "/mypath/sub");
  ASSERT_EQ(TestURI.topic, "");
}

TEST(URI, relpath) {
  URI TestURI("mypath/sub");
  ASSERT_EQ(TestURI.scheme, "");
  ASSERT_EQ(TestURI.host, "");
  ASSERT_EQ(TestURI.port, (uint32_t)0);
  ASSERT_EQ(TestURI.path, "mypath/sub");
  ASSERT_EQ(TestURI.topic, "");
}

TEST(URI, abstopic) {
  URI TestURI("/topic-name.test");
  ASSERT_EQ(TestURI.scheme, "");
  ASSERT_EQ(TestURI.host, "");
  ASSERT_EQ(TestURI.port, (uint32_t)0);
  ASSERT_EQ(TestURI.path, "/topic-name.test");
  ASSERT_EQ(TestURI.topic, "topic-name.test");
}

TEST(URI, reltopic) {
  URI TestURI("topic-name.test");
  ASSERT_EQ(TestURI.scheme, "");
  ASSERT_EQ(TestURI.host, "");
  ASSERT_EQ(TestURI.port, (uint32_t)0);
  ASSERT_EQ(TestURI.path, "topic-name.test");
  ASSERT_EQ(TestURI.topic, "topic-name.test");
}

TEST(URI, host_default_topic) {
  URI TestURI("/some-path");
  TestURI.parse("//my.host");
  ASSERT_EQ(TestURI.scheme, "");
  ASSERT_EQ(TestURI.host, "my.host");
  ASSERT_EQ(TestURI.port, (uint32_t)0);
  ASSERT_EQ(TestURI.path, "/some-path");
  ASSERT_EQ(TestURI.topic, "some-path");
}

TEST(URI, trim) {
  URI TestURI("  //some:123     ");
  ASSERT_EQ(TestURI.host, "some");
  ASSERT_EQ(TestURI.port, 123);
}
