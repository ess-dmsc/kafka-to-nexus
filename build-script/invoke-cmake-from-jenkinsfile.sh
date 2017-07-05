# Factored out from Jenkinsfile because of escape issues

cmake ../code \
-DCMAKE_PREFIX_PATH=../artifacts/graylog-logger/usr/local \
-DCMAKE_INCLUDE_PATH=../googletest\;../streaming-data-types\;$DM_ROOT/usr/include\;$DM_ROOT/usr/lib \
-DCMAKE_LIBRARY_PATH=$DM_ROOT/usr/lib \
-Dflatc=$DM_ROOT/usr/bin/flatc \
-DREQUIRE_GTEST=1 \
-DUSE_GRAYLOG_LOGGER=1
