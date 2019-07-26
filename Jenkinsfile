@Library('ecdc-pipeline')
import ecdcpipeline.ContainerBuildNode
import ecdcpipeline.PipelineBuilder

project = "kafka-to-nexus"


// 'no_graylog' builds code with plain spdlog conan package instead of ess-dmsc spdlog-graylog.
// It fails to build in case graylog functionality was used without prior checking if graylog was available.
clangformat_os = "debian9"
test_and_coverage_os = "centos7"
release_os = "centos7-release"
no_graylog = "centos7-no_graylog"

container_build_nodes = [
  'centos7': ContainerBuildNode.getDefaultContainerBuildNode('centos7'),
  'centos7-release': ContainerBuildNode.getDefaultContainerBuildNode('centos7'),
  'centos7-no_graylog': ContainerBuildNode.getDefaultContainerBuildNode('centos7'),
  'debian9': ContainerBuildNode.getDefaultContainerBuildNode('debian9'),
  'ubuntu1804': ContainerBuildNode.getDefaultContainerBuildNode('ubuntu1804')
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
    numToKeepStr: num_artifacts_to_keep
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
    if (container.key == no_graylog) {
      container.sh """
        mkdir build
          cd build
          conan remote add \
            --insert 0 \
            ${conan_remote} ${local_conan_server}
          conan install --build=outdated ../${pipeline_builder.project}/conan/conanfile_no_graylog.txt
        """
    } else {
    container.sh """
      mkdir build
      cd build
      conan remote add \
        --insert 0 \
        ${conan_remote} ${local_conan_server}
      conan install --build=outdated ../${pipeline_builder.project}/conan/conanfile.txt
    """
    }
  }  // stage

  pipeline_builder.stage("${container.key}: Configure") {
    if (container.key != release_os && container.key != no_graylog) {
      def coverage_on
      if (container.key == test_and_coverage_os) {
        coverage_on = '-DCOV=1'
      } else {
        coverage_on = ''
      }

      def doxygen_on
      if (container.key == clangformat_os) {
        doxygen_on = '-DRUN_DOXYGEN=TRUE'
      } else {
        doxygen_on = ''
      }

      container.sh """
        cd build
        . ./activate_run.sh
        cmake ${coverage_on} ${doxygen_on} ../${pipeline_builder.project}
      """
    } else {
      container.sh """
        cd build
        . ./activate_run.sh
        cmake \
          -DCMAKE_BUILD_TYPE=Release \
          -DCONAN=MANUAL \
          -DCMAKE_SKIP_RPATH=FALSE \
          -DCMAKE_INSTALL_RPATH='\\\\\\\$ORIGIN/../lib' \
          -DCMAKE_BUILD_WITH_INSTALL_RPATH=TRUE \
          -DBUILD_TESTS=FALSE \
          ../${pipeline_builder.project}
      """
    }
  }  // stage

  pipeline_builder.stage("${container.key}: Build") {
    container.sh """
    cd build
    . ./activate_run.sh
    make -j4 all VERBOSE=1
    """
  }  // stage

  pipeline_builder.stage("${container.key}: Test") {
    // env.CHANGE_ID is set for pull request builds.
    if (container.key == test_and_coverage_os && !env.CHANGE_ID) {
      def test_output = "TestResults.xml"
      container.sh """
        cd build
        . ./activate_run.sh
        ./bin/UnitTests -- --gtest_output=xml:${test_output}
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
          python3.6 -m pip install --user codecov
          python3.6 -m codecov -t ${TOKEN} --commit ${scm_vars.GIT_COMMIT} -f ../build/coverage.info
        """
      }  // withCredentials
    } else if (container.key != release_os && container.key != no_graylog) {
      def test_dir
      test_dir = 'bin'

      container.sh """
        cd build
        . ./activate_run.sh
        ./${test_dir}/UnitTests
      """
    }
  }  // stage

  if (container.key == clangformat_os) {
    pipeline_builder.stage("${container.key}: Formatting") {
      if (!env.CHANGE_ID) {
        // Ignore non-PRs
        return
      }
      try {
        container.sh """
          clang-format -version
          cd ${project}
          find . \\\\( -name '*.cpp' -or -name '*.cxx' -or -name '*.h' -or -name '*.hpp' \\\\) \\
          -exec clang-format -i {} +
          git config user.email 'dm-jenkins-integration@esss.se'
          git config user.name 'cow-bot'
          git status -s
          git add -u
          git commit -m 'GO FORMAT YOURSELF'
        """
        withCredentials([
          usernamePassword(
          credentialsId: 'cow-bot-username',
          usernameVariable: 'USERNAME',
          passwordVariable: 'PASSWORD'
          )
        ]) {
          container.sh """
            cd ${project}
            git push https://${USERNAME}:${PASSWORD}@github.com/ess-dmsc/kafka-to-nexus.git HEAD:${CHANGE_BRANCH}
          """
        } // withCredentials
      } catch (e) {
       // Okay to fail as there could be no badly formatted files to commit
      } finally {
        // Clean up
      }
    }  // stage

    pipeline_builder.stage("${container.key}: Cppcheck") {
      def test_output = "cppcheck.xml"
        container.sh """
          cd ${pipeline_builder.project}
          cppcheck --xml --inline-suppr --enable=all --inconclusive src/ 2> ${test_output}
        """
        container.copyFrom("${pipeline_builder.project}/${test_output}", '.')
        recordIssues sourceCodeEncoding: 'UTF-8', qualityGates: [[threshold: 1, type: 'TOTAL', unstable: true]], tools: [cppCheck(pattern: 'cppcheck.xml', reportEncoding: 'UTF-8')]
    }  // stage

    pipeline_builder.stage("${container.key}: Doxygen") {
      def test_output = "doxygen_results.txt"
        container.sh """
          cd build
          pwd
          make docs > ${test_output}
        """
        container.copyFrom("${pipeline_builder.project}/build/${test_output}", '.')
        archiveArtifacts "${test_output}"
    }  // stage
  }  // if

  if (container.key == release_os) {
    pipeline_builder.stage("${container.key}: Formatting") {
      def archive_output = "${pipeline_builder.project}-${container.key}.tar.gz"
      container.sh """
        cd build
        rm -rf ${pipeline_builder.project}; mkdir ${pipeline_builder.project}
        mkdir ${pipeline_builder.project}/bin
        cp ./bin/kafka-to-nexus ${pipeline_builder.project}/bin/
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
          // temporary conan remove until all projects move to new package version
          sh "conan remove -f FlatBuffers/*"
          sh "conan remove -f cli11/*"
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
            sh "make -j4 all UnitTests VERBOSE=1"
            sh ". ./activate_run.sh && ./bin/UnitTests"
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
    node('system-test') {
      cleanWs()
      dir("${project}") {
        try {
          stage("System tests: Checkout") {
            checkout scm
          }  // stage
          stage("System tests: Install requirements") {
            sh """python3.6 -m pip install --user --upgrade pip
           python3.6 -m pip install --user -r system-tests/requirements.txt
            """
          }  // stage
          stage("System tests: Run") {
            // Stop and remove any containers that may have been from the job before,
            // i.e. if a Jenkins job has been aborted.
            sh "docker stop \$(docker ps -a -q) && docker rm \$(docker ps -a -q) || true"
            timeout(time: 30, activity: true){
              sh """cd system-tests/
              python3.6 -m pytest -s --junitxml=./SystemTestsOutput.xml .
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
