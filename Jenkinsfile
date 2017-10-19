def project = "kafka-to-nexus"
def centos = docker.image('essdmscdm/centos-gcc6-build-node:0.1.3')


node('docker') {
    def container_name = "${project}-${env.BRANCH_NAME}-${env.BUILD_NUMBER}"
    def run_args = "\
        --name ${container_name} \
        --tty \
        --env http_proxy=${env.http_proxy} \
        --env https_proxy=${env.https_proxy}"
    def sclsh = "/usr/bin/scl enable rh-python35 devtoolset-6 -- /bin/bash"

    cleanWs()

    dir("${project}") {
	stage('Checkout') {
            scm_vars = checkout scm
	}
    }

    try {
        container = centos.run(run_args)

	// Copy sources to container and change owner and group.
	sh "docker cp ${project} ${container_name}:/home/jenkins/${project}"
	sh """docker exec --user root ${container_name} ${sclsh} -c \"
                chown -R jenkins.jenkins /home/jenkins/${project}
	\""""

        stage('Checkout') {
            def checkout_script = """
                git clone -b master https://github.com/ess-dmsc/streaming-data-types.git
            """
            sh "docker exec ${container_name} ${sclsh} -c \"${checkout_script}\""
        }

        stage('Get Dependencies') {
            def conan_remote = "ess-dmsc-local"
            def dependencies_script = """
                mkdir build
                cd build
                conan remote add \
                    --insert 0 \
                    ${conan_remote} ${local_conan_server}
                conan install ../${project}/conan --build=missing
            """
            sh "docker exec ${container_name} ${sclsh} -c \"${dependencies_script}\""
        }

        stage('Configure') {
            def configure_script = """
                cd build
                cmake3 ../${project} -DREQUIRE_GTEST=ON
            """
            sh "docker exec ${container_name} ${sclsh} -c \"${configure_script}\""
        }

        stage('Build') {
            def build_script = "make --directory=./build VERBOSE=1"
            sh "docker exec ${container_name} ${sclsh} -c \"${build_script}\""
        }

        stage('Test') {
            def test_output = "TestResults.xml"
            def test_script = """
                cd build
                ./tests/tests -- --gtest_output=xml:${test_output}
            """
            sh "docker exec ${container_name} ${sclsh} -c \"${test_script}\""

            // Remove file outside container.
            sh "rm -f ${test_output}"
            // Copy and publish test results.
            sh "docker cp ${container_name}:/home/jenkins/build/${test_output} ."

            junit "${test_output}"
        }

        stage('Archive') {
            def archive_output = "file-writer.tar.gz"
            def archive_script = """
                cd build
                rm -rf file-writer; mkdir file-writer
                cp kafka-to-nexus send-command file-writer/
                tar czf ${archive_output} file-writer
            """
            sh "docker exec ${container_name} ${sclsh} -c \"${archive_script}\""       
            sh "docker cp ${container_name}:/home/jenkins/build/${archive_output} ."
            archiveArtifacts "${archive_output}"
        }
        
    } finally {
        container.stop()
    }
}
