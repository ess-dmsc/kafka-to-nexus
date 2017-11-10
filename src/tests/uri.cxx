#include "../uri.h"
#include <gtest/gtest.h>

using namespace uri;

TEST(URI, host) {
  URI u1("//myhost");
  ASSERT_EQ(u1.scheme, "");
  ASSERT_EQ(u1.host, "myhost");
  ASSERT_EQ(u1.port, (uint32_t)0);
}
TEST(URI, ip) {
  URI u1("//127.0.0.1");
  ASSERT_EQ(u1.scheme, "");
  ASSERT_EQ(u1.host, "127.0.0.1");
  ASSERT_EQ(u1.port, (uint32_t)0);
}
TEST(URI, host_port) {
  URI u1("//myhost:345");
  ASSERT_EQ(u1.scheme, "");
  ASSERT_EQ(u1.host, "myhost");
  ASSERT_EQ(u1.port, (uint32_t)345);
}
TEST(URI, host_port_noslashes) {
  URI u1;
  u1.require_host_slashes = false;
  u1.parse("myhost:345");
  ASSERT_EQ(u1.scheme, "");
  ASSERT_EQ(u1.host, "myhost");
  ASSERT_EQ(u1.port, (uint32_t)345);
}
TEST(URI, ip_port) {
  URI u1("//127.0.0.1:345");
  ASSERT_EQ(u1.scheme, "");
  ASSERT_EQ(u1.host, "127.0.0.1");
  ASSERT_EQ(u1.port, (uint32_t)345);
}
TEST(URI, scheme_host_port) {
  URI u1;
  u1.port = 123;
  u1.parse("http://my.host:345");
  ASSERT_EQ(u1.scheme, "http");
  ASSERT_EQ(u1.host, "my.host");
  ASSERT_EQ(u1.host_port, "my.host:345");
  ASSERT_EQ(u1.port, (uint32_t)345);
}
TEST(URI, scheme_host_port_default) {
  URI u1;
  u1.port = 123;
  u1.parse("http://my.host");
  ASSERT_EQ(u1.scheme, "http");
  ASSERT_EQ(u1.host, "my.host");
  ASSERT_EQ(u1.host_port, "my.host:123");
  ASSERT_EQ(u1.port, (uint32_t)123);
}
TEST(URI, scheme_host_port_pathdefault) {
  URI u1("kafka://my-host.com:8080/");
  ASSERT_EQ(u1.scheme, "kafka");
  ASSERT_EQ(u1.host, "my-host.com");
  ASSERT_EQ(u1.port, (uint32_t)8080);
  ASSERT_EQ(u1.path, "/");
}
TEST(URI, scheme_host_port_path) {
  URI u1("kafka://my-host.com:8080/som_e");
  ASSERT_EQ(u1.scheme, "kafka");
  ASSERT_EQ(u1.host, "my-host.com");
  ASSERT_EQ(u1.port, (uint32_t)8080);
  ASSERT_EQ(u1.path, "/som_e");
  ASSERT_EQ(u1.topic, "som_e");
}
TEST(URI, scheme_host_port_pathlonger) {
  URI u1("kafka://my_host.com:8080/some/longer");
  ASSERT_EQ(u1.scheme, "kafka");
  ASSERT_EQ(u1.host, "my_host.com");
  ASSERT_EQ(u1.port, (uint32_t)8080);
  ASSERT_EQ(u1.path, "/some/longer");
  ASSERT_EQ(u1.topic, "");
}
TEST(URI, host_topic) {
  URI u1("//my.host/the-topic");
  ASSERT_EQ(u1.scheme, "");
  ASSERT_EQ(u1.host, "my.host");
  ASSERT_EQ(u1.port, (uint32_t)0);
  ASSERT_EQ(u1.topic, "the-topic");
}
TEST(URI, host_port_topic) {
  URI u1("//my.host:789/the-topic");
  ASSERT_EQ(u1.scheme, "");
  ASSERT_EQ(u1.host, "my.host");
  ASSERT_EQ(u1.port, (uint32_t)789);
  ASSERT_EQ(u1.topic, "the-topic");
}
TEST(URI, abspath) {
  URI u1("/mypath/sub");
  ASSERT_EQ(u1.scheme, "");
  ASSERT_EQ(u1.host, "");
  ASSERT_EQ(u1.port, (uint32_t)0);
  ASSERT_EQ(u1.path, "/mypath/sub");
  ASSERT_EQ(u1.topic, "");
}
TEST(URI, relpath) {
  URI u1("mypath/sub");
  ASSERT_EQ(u1.scheme, "");
  ASSERT_EQ(u1.host, "");
  ASSERT_EQ(u1.port, (uint32_t)0);
  ASSERT_EQ(u1.path, "mypath/sub");
  ASSERT_EQ(u1.topic, "");
}
TEST(URI, abstopic) {
  URI u1("/topic-name.test");
  ASSERT_EQ(u1.scheme, "");
  ASSERT_EQ(u1.host, "");
  ASSERT_EQ(u1.port, (uint32_t)0);
  ASSERT_EQ(u1.path, "/topic-name.test");
  ASSERT_EQ(u1.topic, "topic-name.test");
}
TEST(URI, reltopic) {
  URI u1("topic-name.test");
  ASSERT_EQ(u1.scheme, "");
  ASSERT_EQ(u1.host, "");
  ASSERT_EQ(u1.port, (uint32_t)0);
  ASSERT_EQ(u1.path, "topic-name.test");
  ASSERT_EQ(u1.topic, "topic-name.test");
}
TEST(URI, host_default_topic) {
  URI u1("/some-path");
  u1.parse("//my.host");
  ASSERT_EQ(u1.scheme, "");
  ASSERT_EQ(u1.host, "my.host");
  ASSERT_EQ(u1.port, (uint32_t)0);
  ASSERT_EQ(u1.path, "/some-path");
  ASSERT_EQ(u1.topic, "some-path");
}
TEST(URI, trim) {
  URI u1("  //some:123     ");
  ASSERT_EQ(u1.host, "some");
  ASSERT_EQ(u1.port, 123);
}
