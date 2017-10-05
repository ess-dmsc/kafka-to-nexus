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

    try {
        container = centos.run(run_args)

        stage('Checkout') {
            def checkout_script = """
                git clone https://github.com/ess-dmsc/${project}.git \
                    --branch ${env.BRANCH_NAME}
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

//           sh "bash ../${project}/build-script/invoke-cmake-from-jenkinsfile.sh"
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

    } finally {
        container.stop()
    }
}
