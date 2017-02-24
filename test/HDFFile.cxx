#include <gtest/gtest.h>
#include "../src/HDFFile.h"

// Verify
TEST(HDFFile, create) {
	using namespace BrightnESS::FileWriter;
	HDFFile f1;
	f1.init("tmp-test.h5");
}
