node('kafka-to-nexus') {
    cleanWs()

    stage("Get artifacts") {
        step([
            $class: 'CopyArtifact',
            filter: 'graylog-logger.tar.gz',
            fingerprintArtifacts: true,
            projectName: 'ess-dmsc/graylog-logger/master',
            target: 'artifacts'
        ])
        dir("artifacts") {
            sh "tar xzf graylog-logger.tar.gz"
        }
    }

    dir("code") {
        stage("Checkout") {
            checkout scm
        }
    }

    dir("build") {
        stage("Update local dependencies") {
            sh "cd .. && bash code/build-script/update-local-deps.sh"
        }

        stage("make clean") {
            sh "rm -rf ../build/*"
        }

        stage("cmake") {
            sh "bash ../code/build-script/invoke-cmake-from-jenkinsfile.sh"
        }

        stage("Build") {
            sh "make VERBOSE=1"
        }

        stage("Test") {
            sh "uname -a"
            sh "ldd ./kafka-to-nexus"
            sh "ldd ./tests/tests"
            sh "./tests/tests"
        }

        stage("Archive") {
            sh "rm -rf file-writer; mkdir file-writer"
            sh "cp kafka-to-nexus send-command file-writer/"
            sh "rm -rf file-writer/libs; mkdir file-writer/libs"
            sh "cp ../artifacts/graylog-logger/usr/local/lib/libgraylog_logger.so file-writer/libs/"
            sh "tar czf file-writer.tar.gz file-writer"
            archiveArtifacts 'file-writer.tar.gz'
        }
    }
}
