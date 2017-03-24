node('kafka-to-nexus') {
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
            sh "make clean; rm CMakeCache.txt; rm -rf ../build/*"
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
