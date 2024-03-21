#include <iostream>
#include <fmt/format.h>
#include "logger.h"
#include "FileWriterTask.h"

namespace {
#define UNUSED_ARG(x) (void)x;
}

int main(int argc, char **argv) {
  UNUSED_ARG(argc);
  UNUSED_ARG(argv);
  std::cout << "hello from the maker app\n";

  return 0;
}
