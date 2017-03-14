node('kafka-to-nexus') {
    dir("code") {
        stage("Checkout") {
            checkout scm
            sh "git submodule update --init"
        }
    }

    dir("build") {
        stage("CMake") {
            sh "rm -rf *"
            sh "HDF5_ROOT=\$DM_ROOT/usr \
                cmake ../code \
                -Dhave_gtest=NO \
                -Dflatc=\$DM_ROOT/usr/bin/flatc \
                -Dpath_include_flatbuffers=\$DM_ROOT/usr/include \
                -Dpath_include_fmt=\$DM_ROOT/usr/include \
                -Dpath_include_rapidjson=\$DM_ROOT/usr/include \
                -Dpath_include_rdkafka=\$DM_ROOT/usr/include \
                -Dpath_lib_rdkafka=\$DM_ROOT/usr/lib/librdkafka.so \
                -Dpath_lib_rdkafka++=\$DM_ROOT/usr/lib/librdkafka++.so \
                -Dpath_include_streaming_data_types=../code/streaming-data-types"
        }

        stage("Build") {
            sh "make VERBOSE=1"
        }

        stage("Archive") {
            sh "mkdir file-writer"
            sh "cp kafka-to-nexus send-command file-writer/"
            sh "tar czf file-writer.tar.gz file-writer"
            archiveArtifacts 'file-writer.tar.gz'
        }
    }
}
