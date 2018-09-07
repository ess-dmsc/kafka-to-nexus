project = "kafka-to-nexus"
clangformat_os = "fedora25"
test_and_coverage_os = "centos7"
release_os = "centos7-release"

images = [
        'centos7': [
                'name': 'essdmscdm/centos7-build-node:3.1.0',
                'sh'  : '/usr/bin/scl enable rh-python35 devtoolset-6 -- /bin/bash -e'
        ],
        'centos7-release': [
                'name': 'essdmscdm/centos7-build-node:3.1.0',
                'sh'  : '/usr/bin/scl enable rh-python35 devtoolset-6 -- /bin/bash -e'
        ],
        'fedora25'    : [
                'name': 'essdmscdm/fedora25-build-node:2.0.0',
                'sh'  : 'bash -e'
        ],
        'ubuntu1804'  : [
                'name': 'essdmscdm/ubuntu18.04-build-node:1.1.0',
                'sh'  : 'bash -e'
        ]
]

base_container_name = "${project}-${env.BRANCH_NAME}-${env.BUILD_NUMBER}"

def Object container_name(image_key) {
    return "${base_container_name}-${image_key}"
}

// Set number of old builds to keep.
properties([[
    $class: 'BuildDiscarderProperty',
    strategy: [
        $class: 'LogRotator',
        artifactDaysToKeepStr: '',
        artifactNumToKeepStr: '10',
        daysToKeepStr: '',
        numToKeepStr: ''
    ]
]]);

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
                        conan install --build=outdated ../${project}/conan/
                    """
        sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${dependencies_script}\""
    } catch (e) {
        failure_function(e, "Add conan remote for (${container_name(image_key)}) failed")
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
                        cmake ../${project} ${coverage_on}
                    """
        sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${configure_script}\""
    } catch (e) {
        failure_function(e, "CMake step for (${container_name(image_key)}) failed")
    }
}

def docker_cmake_release(image_key) {
    try {
        def custom_sh = images[image_key]['sh']
        def configure_script = """
                        cd build
                        . ./activate_run.sh
                        cmake ../${project} \
                            -DCMAKE_BUILD_TYPE=Release \
                            -DCMAKE_SKIP_RPATH=FALSE \
                            -DCMAKE_INSTALL_RPATH='\\\\\\\$ORIGIN/../lib' \
                            -DCMAKE_BUILD_WITH_INSTALL_RPATH=TRUE
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
                      make all UnitTests VERBOSE=1
                  """
        sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${build_script}\""
    } catch (e) {
        failure_function(e, "Build step for (${container_name(image_key)}) failed")
    }
}

def docker_test(image_key, test_dir) {
    try {
        def custom_sh = images[image_key]['sh']
        def test_script = """
                        cd build
                        . ./activate_run.sh
                        ./${test_dir}/UnitTests
                    """
        sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${test_script}\""
    } catch (e) {
        failure_function(e, "Test step for (${container_name(image_key)}) failed")
    }
}

def docker_coverage(image_key) {
    try {
        def custom_sh = images[image_key]['sh']
        def test_output = "TestResults.xml"
        def coverage_script = """
                        cd build
                        . ./activate_run.sh
                        ./tests/UnitTests -- --gtest_output=xml:${test_output}
                        make coverage
                        lcov --directory . --capture --output-file coverage.info
                        lcov --remove coverage.info '*_generated.h' '*/src/date/*' '*/.conan/data/*' '*/usr/*' --output-file coverage.info
                    """
        sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${coverage_script}\""
        sh "docker cp ${container_name(image_key)}:/home/jenkins/build ./"
        junit "build/${test_output}"

        withCredentials([string(credentialsId: 'kafka-to-nexus-codecov-token', variable: 'TOKEN')]) {
            def codecov_upload_script = """
                            cd ${project}
                            export WORKSPACE='.'
                            export JENKINS_URL=${JENKINS_URL}
                            pip install --user codecov
                            python -m codecov -t ${TOKEN} --commit ${scm_vars.GIT_COMMIT} -f ../build/coverage.info
                            """
            sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${codecov_upload_script}\""
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
        def custom_sh = images[image_key]['sh']
        def archive_output = "${project}-${image_key}.tar.gz"
        def archive_script = """
                    cd build
                    rm -rf ${project}; mkdir ${project}
                    mkdir ${project}/bin
                    cp ./bin/{kafka-to-nexus,send-command} ${project}/bin/
                    cp -r ./lib ${project}/
                    cp -r ./licenses ${project}/
                    tar czf ${archive_output} ${project}

                    # Create file with build information
                    touch BUILD_INFO
                    echo 'Repository: ${project}/${env.BRANCH_NAME}' >> BUILD_INFO
                    echo 'Commit: ${scm_vars.GIT_COMMIT}' >> BUILD_INFO
                    echo 'Jenkins build: ${BUILD_NUMBER}' >> BUILD_INFO
                """
        sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${archive_script}\""
        sh "docker cp ${container_name(image_key)}:/home/jenkins/build/${archive_output} ./"
        sh "docker cp ${container_name(image_key)}:/home/jenkins/build/BUILD_INFO ./"
        archiveArtifacts "${archive_output},BUILD_INFO"
    } catch (e) {
        failure_function(e, "Test step for (${container_name(image_key)}) failed")
    }
}

def docker_cppcheck(image_key) {
    try {
        def custom_sh = images[image_key]['sh']
        def test_output = "cppcheck.txt"
        def cppcheck_script = """
                        cd ${project}
                        cppcheck --enable=all --inconclusive --template="{file},{line},{severity},{id},{message}" src/ 2> ${test_output}
                    """
        sh "docker exec ${container_name(image_key)} ${custom_sh} -c \"${cppcheck_script}\""
        sh "docker cp ${container_name(image_key)}:/home/jenkins/${project}/${test_output} ."
    } catch (e) {
        failure_function(e, "Cppcheck step for (${container_name(image_key)}) failed")
    }
}

def get_pipeline(image_key) {
    return {
        stage("${image_key}") {

            try {
                create_container(image_key)

                docker_copy_code(image_key)
                docker_dependencies(image_key)

                if (image_key != release_os) {
                    docker_cmake(image_key)
                }
                else {
                    docker_cmake_release(image_key)
                }

                docker_build(image_key)

                if (image_key == test_and_coverage_os && !env.CHANGE_ID) {
                    docker_coverage(image_key)
                }
                else if (image_key == release_os) {
                    docker_test(image_key, "bin")
                } else {
                    docker_test(image_key, "tests")
                }

                if (image_key == clangformat_os) {
                    docker_formatting(image_key)
                    docker_cppcheck(image_key)
                    step([$class: 'WarningsPublisher', parserConfigurations: [[parserName: 'Cppcheck Parser', pattern: 'cppcheck.txt']]])
                }

                if (image_key == release_os) {
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

                dir("${project}/build") {
                    try {
                        sh "cmake ../code"
                    } catch (e) {
                        failure_function(e, 'MacOSX / CMake failed')
                    }

                    try {
                        sh "make all UnitTests VERBOSE=1"
                        sh ". ./activate_run.sh && ./tests/UnitTests"
                    } catch (e) {
                        failure_function(e, 'MacOSX / build+test failed')
                    }
                }

            }
        }
    }
}

def get_system_tests_pipeline() {
    return {
        node('integration-test') {
            cleanWs()
            dir("${project}") {
                stage("System tests: Checkout") {
                    checkout scm
                }  // stage
                stage("System tests: Install requirements") {
                    sh """scl enable rh-python35 -- python -m pip install --user --upgrade pip
                    scl enable rh-python35 -- python -m pip install --user -r system-tests/requirements.txt
                    """
                }  // stage
                stage("System tests: Run") {
                    sh """cd system-tests/
                    scl enable rh-python35 -- python -m pytest -s --junitxml=./SystemTestsOutput.xml .
                    """
                    junit "system-tests/SystemTestsOutput.xml"
                }  // stage
                stage ("System tests: Clean Up") {
                    sh """rm -rf system-tests/output-files/* || true
                    docker stop \$(\$(docker ps -aq) | grep -E 'kafka|event-producer|zookeeper|filewriter|forwarder') || true
                    docker rm \$(\$(docker ps -aq) | grep -E 'kafka|event-producer|zookeeper|filewriter|forwarder') || true
                    """
                }  // stage
            } // dir
        }  // node
    }  // return
} // def

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

    if ( env.CHANGE_ID ) {
        builders['system tests'] = get_system_tests_pipeline()
    }

    parallel builders

    // Delete workspace when build is done
    cleanWs()
}
