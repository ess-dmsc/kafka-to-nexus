project = "kafka-to-nexus"
clangformat_os = "fedora"
test_and_coverage_os = "centos-gcc6"

images = [
    'centos-gcc6': [
        'name': 'essdmscdm/centos7-gcc6-build-node:2.1.0',
        'sh': '/usr/bin/scl enable rh-python35 devtoolset-6 -- /bin/bash'
    ],
    'fedora': [
        'name': 'essdmscdm/fedora25-build-node:1.0.0',
        'sh': 'sh'
    ],
    'ubuntu1604': [
        'name': 'essdmscdm/ubuntu16.04-build-node:2.1.0',
        'sh': 'sh'
    ]
]

base_container_name = "${project}-${env.BRANCH_NAME}-${env.BUILD_NUMBER}"

def Object container_name(image_key) {
    return "${base_container_name}-${image_key}"
}

def failure_function(exception_obj, failureMessage) {
    def toEmails = [[$class: 'DevelopersRecipientProvider']]
    emailext body: '${DEFAULT_CONTENT}\n\"' + failureMessage + '\"\n\nCheck console output at $BUILD_URL to view the results.', recipientProviders: toEmails, subject: '${DEFAULT_SUBJECT}'
    throw exception_obj
}

def Object get_container(image_key) {
    def image = docker.image(images[image_key]['name'])
    def container = image.run("\
        --name ${container_name(image_key)} \
        --tty \
        --network=host \
        --env http_proxy=${env.http_proxy} \
        --env https_proxy=${env.https_proxy} \
        --env local_conan_server=${env.local_conan_server} \
        ")
    return container
}

def get_pipeline(image_key)
{
    return {
        try {
            def container = get_container(image_key)
            def custom_sh = images[image_key]['sh']

            // Copy sources to container and change owner and group.
            sh "docker cp ${project} ${container_name(image_key)}:/home/jenkins/${project}"
            sh """docker exec --user root ${container_name(image_key)} ${custom_sh} -c \"
                        chown -R jenkins.jenkins /home/jenkins/${project}
            \""""

            if (image_key == clangformat_os) {
            stage('${image_key} Check Formatting') {
                sh """docker exec ${container_name(image_key)} sh -c \"
                    clang-format -version
                    cd ${project}
                    find . \\( -name '*.cpp' -or -name '*.cxx' -or -name '*.h' -or -name '*.hpp' \\) \
                        -exec clangformatdiff.sh {} +
                \""""
            }
            } else {

                stage('${image_key} Checkout Schemas') {
                    def checkout_script = """
                        git clone -b master https://github.com/ess-dmsc/streaming-data-types.git
                    """
                    sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${checkout_script}\""
                }

                stage('${image_key} Get Dependencies') {
                    def conan_remote = "ess-dmsc-local"
                    def dependencies_script = """
                        mkdir build
                        cd build
                        conan remote add \
                            --insert 0 \
                            ${conan_remote} ${local_conan_server}
                        cat ../${project}/CMakeLists.txt
                        conan install --build=outdated ../${project}/conan/conanfile.txt
                    """
                    sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${dependencies_script}\""
                }

                stage('${image_key} Configure') {
                    def coverage_on = ""
                    if (image_key == test_and_coverage_os) {
                        coverage_on = "-DCOV=1"
                    }
                    def configure_script = """
                        cd build
                        . ./activate_run.sh
                        cmake ../${project} -DREQUIRE_GTEST=ON ${coverage_on}
                    """
                    print(configure_script)
                    sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${configure_script}\""
                }

                stage('${image_key} Build') {
                  def build_script = """
                      cd build
                      . ./activate_run.sh
                      make VERBOSE=1
                  """
                  sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${build_script}\""
                }

                stage('${image_key} Test') {
                    def test_output = "TestResults.xml"
                    def test_script = """
                        cd build
                        . ./activate_run.sh
                        ./tests/tests -- --gtest_output=xml:${test_output}
                    """
                    sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${test_script}\""

                    // Remove file outside container.
                    sh "rm -f ${test_output}"
                    // Copy and publish test results (only from one container).
                    if (image_key == test_and_coverage_os) {
                        def coverage_script = """
                            cd build
                            . ./activate_run.sh
                            make coverage
                            lcov --directory . --capture --output-file coverage.info
                            lcov --remove coverage.info '*_generated.h' '*/src/date/*' '*/.conan/data/*' '*/usr/*' --output-file coverage.info
                        """
                        sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${coverage_script}\""
                        sh "docker cp ${container_name(image_key)}:/home/jenkins/build ./"
                        junit "build/${test_output}"

                        print(scm_vars.GIT_COMMIT)
                        withCredentials([string(credentialsId: 'kafka-to-nexus-codecov-token', variable: 'TOKEN')]) {
                            sh "curl -s https://codecov.io/bash | bash -s - -f build/coverage.info -t ${TOKEN} -C ${scm_vars.GIT_COMMIT}"
                        }
                        sh "curl -O https://raw.githubusercontent.com/eriwen/lcov-to-cobertura-xml/master/lcov_cobertura/lcov_cobertura.py"
                        sh "python lcov_cobertura.py build/coverage.info -o build/coverage.xml"

                        sh "head build/coverage.xml"

                        step([
                            $class: 'CoberturaPublisher',
                            autoUpdateHealth: true,
                            autoUpdateStability: true,
                            coberturaReportFile: 'build/coverage.xml',
                            failUnhealthy: false,
                            failUnstable: false,
                            maxNumberOfBuilds: 0,
                            onlyStable: false,
                            sourceEncoding: 'ASCII',
                            zoomCoverageChart: false
                        ])
                    }
                }
            }

            if (image_key == 'centos-gcc6') {
                stage('${image_key} Archive') {
                    def archive_output = "file-writer.tar.gz"
                    def archive_script = """
                        cd build
                        rm -rf file-writer; mkdir file-writer
                        cp kafka-to-nexus send-command file-writer/
                        tar czf ${archive_output} file-writer
                    """
                    sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${archive_script}\""
                    sh "docker cp ${container_name(image_key)}:/home/jenkins/build/${archive_output} ."
                    archiveArtifacts "${archive_output}"
                }
            }
        } catch (e) {
            failure_function(e, "Unknown build failure for ${image_key}")
        } finally {
            sh "docker stop ${container_name(image_key)}"
            sh "docker rm -f ${container_name(image_key)}"
        }
    }
}

node('docker') {
  cleanWs()

  stage('Checkout') {
      dir("${project}") {
          try {
              scm_vars = checkout scm
          } catch (e) {
              failure_function(e, 'Checkout failed')
          }
      }
  }

  def builders = [:]
  for (x in images.keySet()) {
      def image_key = x
      builders[image_key] = get_pipeline(image_key)
  }
  parallel builders

  // Delete workspace when build is done
  cleanWs()
}
