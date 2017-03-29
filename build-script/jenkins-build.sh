find . -maxdepth 3
rm -rf build
mkdir -p build
cd build
cmake -DCMAKE_INCLUDE_PATH=../repos/streaming-data-types\;../repos/googletest -DCMAKE_INSTALL_PREFIX=`cd ../install; pwd` -DREQUIRE_GTEST=1 ../repos/kafka-to-nexus  &&  make VERBOSE=1
