#include "URI.h"
#include <gtest/gtest.h>

using namespace uri;

TEST(URI, host) {
  URI TestURI("//myhost");
  ASSERT_EQ(TestURI.Scheme, "");
  ASSERT_EQ(TestURI.Host, "myhost");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
}

TEST(URI, ip) {
  URI TestURI("//127.0.0.1");
  ASSERT_EQ(TestURI.Scheme, "");
  ASSERT_EQ(TestURI.Host, "127.0.0.1");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
}

TEST(URI, host_port) {
  URI TestURI("//myhost:345");
  ASSERT_EQ(TestURI.Scheme, "");
  ASSERT_EQ(TestURI.Host, "myhost");
  ASSERT_EQ(TestURI.Port, (uint32_t)345);
}

TEST(URI, expectedURIStringWhenHostPortAndTopicAreSpecified) {
  std::string TestURIString = "//myhost:345/mytopic";
  URI TestURI(TestURIString);
  ASSERT_EQ(TestURI.getURIString(), TestURIString);
}

TEST(URI, host_port_noslashes) {
  URI TestURI;
  TestURI.RequireHostSlashes = false;
  TestURI.parse("myhost:345");
  ASSERT_EQ(TestURI.Scheme, "");
  ASSERT_EQ(TestURI.Host, "myhost");
  ASSERT_EQ(TestURI.Port, (uint32_t)345);
}

TEST(URI, ip_port) {
  URI TestURI("//127.0.0.1:345");
  ASSERT_EQ(TestURI.Scheme, "");
  ASSERT_EQ(TestURI.Host, "127.0.0.1");
  ASSERT_EQ(TestURI.Port, (uint32_t)345);
}

TEST(URI, scheme_host_port) {
  URI TestURI;
  TestURI.Port = 123;
  TestURI.parse("http://my.Host:345");
  ASSERT_EQ(TestURI.Scheme, "http");
  ASSERT_EQ(TestURI.Host, "my.Host");
  ASSERT_EQ(TestURI.HostPort, "my.Host:345");
  ASSERT_EQ(TestURI.Port, (uint32_t)345);
}

TEST(URI, scheme_host_port_default) {
  URI TestURI;
  TestURI.Port = 123;
  TestURI.parse("http://my.Host");
  ASSERT_EQ(TestURI.Scheme, "http");
  ASSERT_EQ(TestURI.Host, "my.Host");
  ASSERT_EQ(TestURI.HostPort, "my.Host:123");
  ASSERT_EQ(TestURI.Port, (uint32_t)123);
}

TEST(URI, scheme_host_port_pathdefault) {
  URI TestURI("kafka://my-Host.com:8080/");
  ASSERT_EQ(TestURI.Scheme, "kafka");
  ASSERT_EQ(TestURI.Host, "my-Host.com");
  ASSERT_EQ(TestURI.Port, (uint32_t)8080);
  ASSERT_EQ(TestURI.Path, "/");
}

TEST(URI, scheme_host_port_path) {
  URI TestURI("kafka://my-Host.com:8080/som_e");
  ASSERT_EQ(TestURI.Scheme, "kafka");
  ASSERT_EQ(TestURI.Host, "my-Host.com");
  ASSERT_EQ(TestURI.Port, (uint32_t)8080);
  ASSERT_EQ(TestURI.Path, "/som_e");
  ASSERT_EQ(TestURI.Topic, "som_e");
}

TEST(URI, scheme_host_port_pathlonger) {
  URI TestURI("kafka://my_host.com:8080/some/longer");
  ASSERT_EQ(TestURI.Scheme, "kafka");
  ASSERT_EQ(TestURI.Host, "my_host.com");
  ASSERT_EQ(TestURI.Port, (uint32_t)8080);
  ASSERT_EQ(TestURI.Path, "/some/longer");
  ASSERT_EQ(TestURI.Topic, "");
}

TEST(URI, host_topic) {
  URI TestURI("//my.Host/the-topic");
  ASSERT_EQ(TestURI.Scheme, "");
  ASSERT_EQ(TestURI.Host, "my.Host");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
  ASSERT_EQ(TestURI.Topic, "the-topic");
}

TEST(URI, host_port_topic) {
  URI TestURI("//my.Host:789/the-topic");
  ASSERT_EQ(TestURI.Scheme, "");
  ASSERT_EQ(TestURI.Host, "my.Host");
  ASSERT_EQ(TestURI.Port, (uint32_t)789);
  ASSERT_EQ(TestURI.Topic, "the-topic");
}

TEST(URI, abspath) {
  URI TestURI("/mypath/sub");
  ASSERT_EQ(TestURI.Scheme, "");
  ASSERT_EQ(TestURI.Host, "");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
  ASSERT_EQ(TestURI.Path, "/mypath/sub");
  ASSERT_EQ(TestURI.Topic, "");
}

TEST(URI, relpath) {
  URI TestURI("mypath/sub");
  ASSERT_EQ(TestURI.Scheme, "");
  ASSERT_EQ(TestURI.Host, "");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
  ASSERT_EQ(TestURI.Path, "mypath/sub");
  ASSERT_EQ(TestURI.Topic, "");
}

TEST(URI, abstopic) {
  URI TestURI("/topic-name.test");
  ASSERT_EQ(TestURI.Scheme, "");
  ASSERT_EQ(TestURI.Host, "");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
  ASSERT_EQ(TestURI.Path, "/topic-name.test");
  ASSERT_EQ(TestURI.Topic, "topic-name.test");
}

TEST(URI, reltopic) {
  URI TestURI("topic-name.test");
  ASSERT_EQ(TestURI.Scheme, "");
  ASSERT_EQ(TestURI.Host, "");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
  ASSERT_EQ(TestURI.Path, "topic-name.test");
  ASSERT_EQ(TestURI.Topic, "topic-name.test");
}

TEST(URI, host_default_topic) {
  URI TestURI("/some-path");
  TestURI.parse("//my.Host");
  ASSERT_EQ(TestURI.Scheme, "");
  ASSERT_EQ(TestURI.Host, "my.Host");
  ASSERT_EQ(TestURI.Port, (uint32_t)0);
  ASSERT_EQ(TestURI.Path, "/some-path");
  ASSERT_EQ(TestURI.Topic, "some-path");
}

TEST(URI, trim) {
  URI TestURI("  //some:123     ");
  ASSERT_EQ(TestURI.Host, "some");
  ASSERT_EQ(TestURI.Port, 123u);
}
