@Library('ecdc-pipeline')
import ecdcpipeline.ContainerBuildNode
import ecdcpipeline.PipelineBuilder

project = "kafka-to-nexus"

clangformat_os = "debian9"
test_and_coverage_os = "centos7"
release_os = "centos7-release"

container_build_nodes = [
  'centos7': new ContainerBuildNode('essdmscdm/centos7-build-node:3.2.0', '/usr/bin/scl enable rh-python35 devtoolset-6 -- /bin/bash -e'),
  'centos7-release': new ContainerBuildNode('essdmscdm/centos7-build-node:3.2.0', '/usr/bin/scl enable rh-python35 devtoolset-6 -- /bin/bash -e'),
  'debian9': new ContainerBuildNode('essdmscdm/debian9-build-node:2.2.0', 'bash -e'),
  'ubuntu1804': new ContainerBuildNode('essdmscdm/ubuntu18.04-build-node:1.2.0', 'bash -e')
]

// Define number of old builds to keep. These numbers are somewhat arbitrary,
// but based on the fact that for the master branch we want to have a certain
// number of old builds available, while for the other branches we want to be
// able to deploy easily without using too much disk space.
def num_artifacts_to_keep
if (env.BRANCH_NAME == 'master') {
  num_artifacts_to_keep = '5'
} else {
  num_artifacts_to_keep = '1'
}

// Set number of old builds to keep.
properties([[
  $class: 'BuildDiscarderProperty',
  strategy: [
    $class: 'LogRotator',
    artifactDaysToKeepStr: '',
    artifactNumToKeepStr: num_artifacts_to_keep,
    daysToKeepStr: '',
    numToKeepStr: ''
  ]
]]);


pipeline_builder = new PipelineBuilder(this, container_build_nodes)
pipeline_builder.activateEmailFailureNotifications()

builders = pipeline_builder.createBuilders { container ->
  pipeline_builder.stage("${container.key}: Checkout") {
    dir(pipeline_builder.project) {
      scm_vars = checkout scm
    }
    container.copyTo(pipeline_builder.project, pipeline_builder.project)
  }  // stage

  pipeline_builder.stage("${container.key}: Dependencies") {
    def conan_remote = "ess-dmsc-local"
    container.sh """
      mkdir build
      cd build
      conan remote add \
        --insert 0 \
        ${conan_remote} ${local_conan_server}
      conan install --build=outdated ../${pipeline_builder.project}/conan
    """
  }  // stage

  pipeline_builder.stage("${container.key}: Configure") {
    if (container.key != release_os) {
      def coverage_on
      if (container.key == test_and_coverage_os) {
        coverage_on = '-DCOV=1'
      } else {
        coverage_on = ''
      }

      container.sh """
        cd build
        . ./activate_run.sh
        cmake ${coverage_on} ../${pipeline_builder.project}
      """
    } else {
      container.sh """
        cd build
        . ./activate_run.sh
        cmake \
          -DCMAKE_BUILD_TYPE=Release \
          -DCMAKE_SKIP_RPATH=FALSE \
          -DCMAKE_INSTALL_RPATH='\\\\\\\$ORIGIN/../lib' \
          -DCMAKE_BUILD_WITH_INSTALL_RPATH=TRUE \
          ../${pipeline_builder.project}
      """
    }
  }  // stage

  pipeline_builder.stage("${container.key}: Build") {
    container.sh """
      cd build
      . ./activate_run.sh
      make all UnitTests VERBOSE=1
    """
  }  // stage

  pipeline_builder.stage("${container.key}: Test") {
    // env.CHANGE_ID is set for pull request builds.
    if (container.key == test_and_coverage_os && !env.CHANGE_ID) {
      def test_output = "TestResults.xml"
      container.sh """
        cd build
        . ./activate_run.sh
        ./tests/UnitTests -- --gtest_output=xml:${test_output}
        make coverage
        lcov --directory . --capture --output-file coverage.info
        lcov --remove coverage.info '*_generated.h' '*/src/date/*' '*/.conan/data/*' '*/usr/*' --output-file coverage.info
      """

      container.copyFrom('build', '.')
      junit "build/${test_output}"

      withCredentials([
        string(
          credentialsId: 'kafka-to-nexus-codecov-token',
          variable: 'TOKEN'
        )
      ]) {
        container.sh """
          cd ${pipeline_builder.project}
          export WORKSPACE='.'
          export JENKINS_URL=${JENKINS_URL}
          pip install --user codecov
          python -m codecov -t ${TOKEN} --commit ${scm_vars.GIT_COMMIT} -f ../build/coverage.info
        """
      }  // withCredentials
    } else {
      def test_dir
      if (container.key == release_os) {
        test_dir = 'bin'
      } else {
        test_dir = 'tests'
      }

      container.sh """
        cd build
        . ./activate_run.sh
        ./${test_dir}/UnitTests
      """
    }
  }  // stage

  if (container.key == clangformat_os) {
    pipeline_builder.stage("${container.key}: Formatting") {
      container.sh """
        clang-format -version
        cd ${pipeline_builder.project}
        find . \\\\( -name '*.cpp' -or -name '*.cxx' -or -name '*.h' -or -name '*.hpp' \\\\) \\
          -exec clangformatdiff.sh {} +
      """
    }  // stage

    pipeline_builder.stage("${container.key}: Cppcheck") {
      def test_output = "cppcheck.txt"
      container.sh """
        cd ${pipeline_builder.project}
        cppcheck --enable=all --inconclusive --template="{file},{line},{severity},{id},{message}" src/ 2> ${test_output}
      """

      container.copyFrom("${pipeline_builder.project}/${test_output}", '.')
      step([
        $class: 'WarningsPublisher',
        parserConfigurations: [[
          parserName: 'Cppcheck Parser',
          pattern: 'cppcheck.txt'
        ]]
      ])
    }  // stage
  }  // if

  if (container.key == release_os) {
    pipeline_builder.stage("${container.key}: Formatting") {
      def archive_output = "${pipeline_builder.project}-${container.key}.tar.gz"
      container.sh """
        cd build
        rm -rf ${pipeline_builder.project}; mkdir ${pipeline_builder.project}
        mkdir ${pipeline_builder.project}/bin
        cp ./bin/{kafka-to-nexus,send-command} ${pipeline_builder.project}/bin/
        cp -r ./lib ${pipeline_builder.project}/
        cp -r ./licenses ${pipeline_builder.project}/
        tar czf ${archive_output} ${pipeline_builder.project}

        # Create file with build information
        touch BUILD_INFO
        echo 'Repository: ${pipeline_builder.project}/${env.BRANCH_NAME}' >> BUILD_INFO
        echo 'Commit: ${scm_vars.GIT_COMMIT}' >> BUILD_INFO
        echo 'Jenkins build: ${env.BUILD_NUMBER}' >> BUILD_INFO
      """

      container.copyFrom("build/${archive_output}", '.')
      container.copyFrom('build/BUILD_INFO', '.')
      archiveArtifacts "${archive_output},BUILD_INFO"
    }  // stage
  }  // if
}  // createBuilders

node {
  dir("${project}") {
    try {
      scm_vars = checkout scm
    } catch (e) {
      failure_function(e, 'Checkout failed')
    }
  }

  builders['macOS'] = get_macos_pipeline()

  if ( env.CHANGE_ID ) {
      builders['system tests'] = get_system_tests_pipeline()
  }

  try {
    parallel builders
  } catch (e) {
    pipeline_builder.handleFailureMessages()
    throw e
  }

  // Delete workspace when build is done
  cleanWs()
}

def failure_function(exception_obj, failureMessage) {
  def toEmails = [[$class: 'DevelopersRecipientProvider']]
  emailext body: '${DEFAULT_CONTENT}\n\"' + failureMessage + '\"\n\nCheck console output at $BUILD_URL to view the results.', recipientProviders: toEmails, subject: '${DEFAULT_SUBJECT}'
  slackSend color: 'danger', message: "${project}: " + failureMessage
  throw exception_obj
}

def get_macos_pipeline() {
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
        try {
          stage("System tests: Checkout") {
            checkout scm
          }  // stage
          stage("System tests: Install requirements") {
            sh """scl enable rh-python35 -- python -m pip install --user --upgrade pip
            scl enable rh-python35 -- python -m pip install --user -r system-tests/requirements.txt
            """
          }  // stage
          stage("System tests: Run") {
            // Stop and remove any containers that may have been from the job before,
            // i.e. if a Jenkins job has been aborted.
            sh "docker stop \$(docker ps -a -q) && docker rm \$(docker ps -a -q) || true"
            timeout(time: 30, activity: true){
              sh """cd system-tests/
              scl enable rh-python35 -- python -m pytest -s --junitxml=./SystemTestsOutput.xml .
              """
            }
          }  // stage
        } finally {
          stage ("System tests: Clean Up") {
            // The statements below return true because the build should pass
            // even if there are no docker containers or output files to be
            // removed.
            sh """rm -rf system-tests/output-files/* || true
            docker stop \$(docker ps -a -q) && docker rm \$(docker ps -a -q) || true
            """
          }  // stage
          stage("System tests: Archive") {
            junit "system-tests/SystemTestsOutput.xml"
            archiveArtifacts "system-tests/logs/*.log"
          }
        }  // try/finally
      } // dir
    }  // node
  }  // return
} // def
