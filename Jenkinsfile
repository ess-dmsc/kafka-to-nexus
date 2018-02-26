project = "kafka-to-nexus"
clangformat_os = "fedora25"
test_and_coverage_os = "centos7-gcc6"
archive_os = "centos7-gcc6"

images = [
        'centos7-gcc6': [
                'name': 'essdmscdm/centos7-gcc6-build-node:2.1.0',
                'sh'  : '/usr/bin/scl enable rh-python35 devtoolset-6 -- /bin/bash'
        ],
        'fedora25'    : [
                'name': 'essdmscdm/fedora25-build-node:1.0.0',
                'sh'  : 'sh'
        ],
        'ubuntu1604'  : [
                'name': 'essdmscdm/ubuntu16.04-build-node:2.1.0',
                'sh'  : 'sh'
        ],
        'ubuntu1710': [
                'name': 'essdmscdm/ubuntu17.10-build-node:2.0.0',
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
    slackSend color: 'danger', message: "${project}: " + failureMessage
    throw exception_obj
}

def create_container(image_key) {
    def image = docker.image(images[image_key]['name'])
    def container = image.run("\
        --name ${container_name(image_key)} \
        --tty \
        --network=host \
        --env http_proxy=${env.http_proxy} \
        --env https_proxy=${env.https_proxy} \
        --env local_conan_server=${env.local_conan_server} \
          ")
}

def docker_copy_code(image_key) {
    def custom_sh = images[image_key]['sh']
    sh "docker cp ${project} ${container_name(image_key)}:/home/jenkins/${project}"
    sh """docker exec --user root ${container_name(image_key)} ${custom_sh} -c \"
                        chown -R jenkins.jenkins /home/jenkins/${project}
                        \""""
}

def docker_dependencies(image_key) {
    try {
        def custom_sh = images[image_key]['sh']
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

        def checkout_script = """
                        git clone -b master https://github.com/ess-dmsc/streaming-data-types.git
                    """
        sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${checkout_script}\""
    } catch (e) {
        failure_function(e, "Get dependencies for (${container_name(image_key)}) failed")
    }
}

def docker_cmake(image_key) {
    try {
        def custom_sh = images[image_key]['sh']
        def coverage_on = ""
        if (image_key == test_and_coverage_os) {
            coverage_on = "-DCOV=1"
        }
        def configure_script = """
                        cd build
                        . ./activate_run.sh
                        cmake ../${project} -DREQUIRE_GTEST=ON ${coverage_on}
                    """
        sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${configure_script}\""
    } catch (e) {
        failure_function(e, "CMake step for (${container_name(image_key)}) failed")
    }
}

def docker_build(image_key) {
    try {
        def custom_sh = images[image_key]['sh']
        def build_script = """
                      cd build
                      . ./activate_run.sh
                      make VERBOSE=1
                  """
        sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${build_script}\""
    } catch (e) {
        failure_function(e, "Build step for (${container_name(image_key)}) failed")
    }
}

def docker_test(image_key) {
    try {
        def custom_sh = images[image_key]['sh']
        def test_script = """
                        cd build
                        . ./activate_run.sh
                        ./tests/tests
                    """
        sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${test_script}\""
    } catch (e) {
        failure_function(e, "Test step for (${container_name(image_key)}) failed")
    }
}

def docker_coverage(image_key) {
    try {
        dir("${image_key}") {
            def custom_sh = images[image_key]['sh']
            def test_output = "TestResults.xml"
            def coverage_script = """
                            cd build
                            . ./activate_run.sh
                            ./tests/tests -- --gtest_output=xml:${test_output}
                            make coverage
                        """
            sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${coverage_script}\""
            sh "docker cp ${container_name(image_key)}:/home/jenkins/build ./"
            junit "build/${test_output}"

            withCredentials([string(credentialsId: 'kafka-to-nexus-codecov-token', variable: 'TOKEN')]) {
                sh "cd build && curl -s https://codecov.io/bash | bash -s - -t ${TOKEN} -C ${scm_vars.GIT_COMMIT}"
            }
        }
    } catch (e) {
        failure_function(e, "Coverage step for (${container_name(image_key)}) failed")
    }
}

def docker_formatting(image_key) {
    try {
        def custom_sh = images[image_key]['sh']
        def script = """
                    clang-format -version
                    cd ${project}
                    find . \\\\( -name '*.cpp' -or -name '*.cxx' -or -name '*.h' -or -name '*.hpp' \\\\) \\
                        -exec clangformatdiff.sh {} +
                  """
        sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${script}\""
    } catch (e) {
        failure_function(e, "Check formatting step for (${container_name(image_key)}) failed")
    }
}

def docker_archive(image_key) {
    try {
        dir("${image_key}") {
            def custom_sh = images[image_key]['sh']
            def archive_output = "file-writer.tar.gz"
            def archive_script = """
                        cd build
                        rm -rf file-writer; mkdir file-writer
                        cp kafka-to-nexus send-command file-writer/
                        tar czf ${archive_output} file-writer
                    """
            sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${archive_script}\""
            sh "docker cp ${container_name(image_key)}:/home/jenkins/build/${archive_output} ./"
            archiveArtifacts "${archive_output}"
        }
    } catch (e) {
        failure_function(e, "Test step for (${container_name(image_key)}) failed")
    }
}


def get_pipeline(image_key) {
    return {
        stage("${image_key}") {

            try {
                create_container(image_key)

                docker_copy_code(image_key)
                docker_dependencies(image_key)
                docker_cmake(image_key)
                docker_build(image_key)

                if (image_key == test_and_coverage_os) {
                    docker_coverage(image_key)
                }
                else {
                    docker_test(image_key)
                }

                if (image_key == clangformat_os) {
                    docker_formatting(image_key)
                }

                if (image_key == archive_os) {
                    docker_archive(image_key)
                }

            } catch (e) {
                failure_function(e, "Unknown build failure for ${image_key}")
            } finally {
                sh "docker stop ${container_name(image_key)}"
                sh "docker rm -f ${container_name(image_key)}"
            }
        }
    }
}

def get_macos_pipeline()
{
    return {
        stage("macOS") {
            node ("macos") {
                // Delete workspace when build is done
                cleanWs()

                dir("${project}/code") {
                    try {
                        checkout scm
                    } catch (e) {
                        failure_function(e, 'MacOSX / Checkout failed')
                    }
                }

                dir("${project}") {
                    sh "git clone -b master https://github.com/ess-dmsc/streaming-data-types.git"
                }

                dir("${project}/build") {
                    try {
                        sh "conan install --build=outdated ../code/conan"
                    } catch (e) {
                        failure_function(e, 'MacOSX / getting dependencies failed')
                    }

                    try {
                        sh "cmake -DREQUIRE_GTEST=ON ../code"
                    } catch (e) {
                        failure_function(e, 'MacOSX / CMake failed')
                    }

                    try {
                        sh "make VERBOSE=1"
                        sh "./tests/tests"
                    } catch (e) {
                        failure_function(e, 'MacOSX / build+test failed')
                    }
                }

            }
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
    builders['macOS'] = get_macos_pipeline()

    parallel builders

    // Delete workspace when build is done
    cleanWs()
}
