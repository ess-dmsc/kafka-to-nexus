#include "StreamerI.h"

#include <gtest/gtest.h>

TEST(SourcesHandler, AddSources) {
  FileWriter::SourcesHandler Handler;
  ASSERT_EQ(Handler.numSources(), 0ul);
  Handler.addSource("topic-0");
  EXPECT_EQ(Handler.numSources(), 1ul);
  Handler.addSource("topic-1");
  EXPECT_EQ(Handler.numSources(), 2ul);
}

TEST(SourcesHandler, VerifySourcePresent) {
  FileWriter::SourcesHandler Handler;
  Handler.addSource("topic-0");
  Handler.addSource("topic-1");
  ASSERT_EQ(Handler.numSources(), 2ul);

  EXPECT_FALSE(Handler.isPresent("topic-missing"));
  EXPECT_TRUE(Handler.isPresent("topic-1"));
}

TEST(SourcesHandler, IgnoreSourcesIfAlreadyPresent) {
  FileWriter::SourcesHandler Handler;
  ASSERT_EQ(Handler.numSources(), 0ul);
  Handler.addSource("topic-0");
  EXPECT_EQ(Handler.numSources(), 1ul);
  Handler.addSource("topic-0");
  EXPECT_EQ(Handler.numSources(), 1ul);
}

TEST(SourcesHandler, RemoveSources) {
  FileWriter::SourcesHandler Handler;
  Handler.addSource("topic-0");
  Handler.addSource("topic-1");
  ASSERT_EQ(Handler.numSources(), 2ul);

  Handler.removeSource("topic-0");
  EXPECT_EQ(Handler.numSources(), 1ul);
  Handler.removeSource("topic-1");
  EXPECT_EQ(Handler.numSources(), 0ul);
}

TEST(SourcesHandler, IgnoreRemoveSourcesIfNotPresent) {
  FileWriter::SourcesHandler Handler;
  Handler.addSource("topic-0");
  Handler.addSource("topic-1");
  ASSERT_EQ(Handler.numSources(), 2ul);

  Handler.removeSource("topic-missing");
  EXPECT_EQ(Handler.numSources(), 2ul);
}
