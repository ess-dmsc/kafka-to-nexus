node('kafka-to-nexus') {
    stage("Get artifacts") {
        step([
            $class: 'CopyArtifact',
            filter: 'graylog-logger.tar.gz',
            fingerprintArtifacts: true,
            projectName: 'ess-dmsc/graylog-logger/master',
            target: 'artifacts'
        ])
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

        stage("Archive") {
            sh "rm -rf file-writer; mkdir file-writer"
            sh "cp kafka-to-nexus send-command file-writer/"
            sh "tar czf file-writer.tar.gz file-writer"
            archiveArtifacts 'file-writer.tar.gz'
        }
    }
}
