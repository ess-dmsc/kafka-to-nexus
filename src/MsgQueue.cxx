#include "MsgQueue.h"

void swap(MsgQueue &x, MsgQueue &y) {
  using std::swap;
  swap(x.items, y.items);
}
