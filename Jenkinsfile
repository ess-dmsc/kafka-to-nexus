@Library('ecdc-pipeline')
import ecdcpipeline.ContainerBuildNode
import ecdcpipeline.PipelineBuilder

project = "kafka-to-nexus"

// Define number of old builds to keep. These numbers are somewhat arbitrary,
// but based on the fact that for the main branch we want to have a certain
// number of old builds available, while for the other branches we want to be
// able to deploy easily without using too much disk space.
def num_artifacts_to_keep
if (env.BRANCH_NAME == 'main') {
  num_artifacts_to_keep = '5'
} else {
  num_artifacts_to_keep = '2'
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

def checkout(builder, container) {
    builder.stage("${container.key}: Checkout") {
        dir(builder.project) {
          scm_vars = checkout scm
        }
        container.copyTo(builder.project, builder.project)
    }
}

def cpp_dependencies(builder, container) {
    builder.stage("${container.key}: Dependencies") {

        container.sh """
          mkdir build
          cd build
          conan install --build=outdated ../${builder.project}/conanfile.txt
          conan info ../${builder.project}/conanfile.txt > CONAN_INFO
        """
    }
}

def build(builder, container, unit_tests=false) {
    String Target = "kafka-to-nexus"
    if (unit_tests) {
        Target = "UnitTests"
    }
    builder.stage("${container.key}: Build") {
        container.sh """
        cd build
        . ./activate_run.sh
        ninja ${Target}
        """
    }
}

def unit_tests(builder, container, coverage) {
    builder.stage("${container.key}: Test") {
        // env.CHANGE_ID is set for pull request builds.
        if (coverage) {
          def test_output = "TestResults.xml"
          container.sh """
            cd build
            . ./activate_run.sh
            ./bin/UnitTests -- --gtest_output=xml:${test_output}
            ninja coverage
          """

          container.copyFrom('build', '.')
          junit "build/${test_output}"

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
              zoomCoverageChart: true
          ])

          if (env.CHANGE_ID) {
            coverage_summary = sh (
                script: "sed -n -e '/^TOTAL/p' build/coverage.txt",
                returnStdout: true
            ).trim()

            def repository_url = scm.userRemoteConfigs[0].url
            def repository_name = repository_url.replace("git@github.com:","").replace(".git","").replace("https://github.com/","")
            def comment_text = "**Code Coverage**\\n*(Lines    Exec  Cover)*\\n${coverage_summary}\\n*For more detail see Cobertura report in Jenkins interface*"

            withCredentials([usernamePassword(credentialsId: 'cow-bot-username-with-token', usernameVariable: 'UNUSED_VARIABLE', passwordVariable: 'GITHUB_TOKEN')]) {
              withEnv(["comment_text=${comment_text}", "repository_name=${repository_name}"]) {
                sh 'curl -s -H "Authorization: token $GITHUB_TOKEN" -X POST -d "{\\"body\\": \\"$comment_text\\"}" "https://api.github.com/repos/$repository_name/issues/$CHANGE_ID/comments"'
              }
            }
          }

        } else {
          def test_dir
          test_dir = 'bin'

          container.sh """
            cd build
            . ./activate_run.sh
            ./${test_dir}/UnitTests
          """
        }
      }  // stage
}

def configure(builder, container, extra_flags, release_build) {
    builder.stage("${container.key}: Configure") {
        if (!release_build) {
          container.sh """
            cd build
            . ./activate_run.sh
            cmake ${extra_flags} -GNinja ../${builder.project}
          """
        } else {

          container.sh """
            cd build
            . ./activate_run.sh
            cmake \
              -GNinja \
              -DCMAKE_BUILD_TYPE=Release \
              -DCONAN=MANUAL \
              -DCMAKE_SKIP_RPATH=FALSE \
              -DCMAKE_INSTALL_RPATH='\\\\\\\$ORIGIN/../lib' \
              -DCMAKE_BUILD_WITH_INSTALL_RPATH=TRUE \
              ../${builder.project}
          """
        }
    }
}

def static_checks(builder, container) {
    builder.stage("${container.key}: Formatting") {
          if (!env.CHANGE_ID) {
            // Ignore non-PRs
            return
          }

          container.setupLocalGitUser(builder.project)

          try {
            // Do clang-format of C++ files
            container.sh """
              clang-format -version
              cd ${project}
              find . \\\\( -name '*.cpp' -or -name '*.cxx' -or -name '*.h' -or -name '*.hpp' \\\\) \\
              -exec clang-format -i {} +
              git status -s
              git add -u
              git commit -m 'GO FORMAT YOURSELF (clang-format)'
            """
          } catch (e) {
          // Okay to fail as there could be no badly formatted files to commit
          } finally {
          // Clean up
          }

          try {
            // Do black format of python scripts
            container.sh """
              /home/jenkins/.local/bin/black --version
              cd ${project}
              /home/jenkins/.local/bin/black integration-tests
              git status -s
              git add -u
              git commit -m 'GO FORMAT YOURSELF (black)'
            """
          } catch (e) {
          // Okay to fail as there could be no badly formatted files to commit
          } finally {
          // Clean up
          }

          // Push any changes resulting from formatting
          container.copyFrom(pipeline_builder.project, 'clang-formatted-code')
          try {
            withCredentials([
              gitUsernamePassword(
                credentialsId: 'cow-bot-username-with-token',
                gitToolName: 'Default'
              )
            ]) {
              withEnv(["PROJECT=${builder.project}"]) {
                sh '''
                  cd clang-formatted-code
                  git push https://github.com/ess-dmsc/$PROJECT.git HEAD:$CHANGE_BRANCH
                '''
              }  // withEnv
            }  // withCredentials
          } catch (e) {
          // Okay to fail; there may be nothing to push
          } finally {
          // Clean up
          }
        }  // stage

        builder.stage("${container.key}: Cppcheck") {
          def test_output = "cppcheck.xml"
            container.sh """
              cd ${builder.project}
              cppcheck --xml --inline-suppr --suppress=unusedFunction --suppress=missingInclude --enable=all --inconclusive src/ 2> ${test_output}
            """
            container.copyFrom("${builder.project}/${test_output}", builder.project)
            dir("${builder.project}") {
              sh "cat ${test_output} || true"
              recordIssues quiet: true, sourceCodeEncoding: 'UTF-8', qualityGates: [[threshold: 1, type: 'TOTAL', unstable: true]], tools: [cppCheck(pattern: 'cppcheck.xml', reportEncoding: 'UTF-8')]
            }
        }  // stage

        builder.stage("${container.key}: Doxygen") {
          def test_output = "doxygen_results.txt"
            container.sh """
              cd build
              pwd
              ninja docs 2>&1 > ${test_output}
            """
            container.copyFrom("build/${test_output}", '.')

            String regexMissingDocsMemberConsumer = /.* warning: Member Consumer .* of class .* is not documented .*/
            String regexInvalidArg = /.* warning: argument: .* is not found in the argument list of .*/
            String regexMissingCompoundDocs = /.*warning: Compound .* is not documented./
            boolean doxygenStepFailed = false
            def failingCases = []
            def doxygenResultContent = readFile test_output
            def doxygenResultLines = doxygenResultContent.readLines()

            for(line in doxygenResultLines) {
                if(line ==~ regexMissingDocsMemberConsumer || line ==~ regexInvalidArg ||
                    line ==~ regexMissingCompoundDocs) {
                    failingCases.add(line)
                    }
            }

            int acceptableFailedCases = 75
            int numFailedCases = failingCases.size()
            if(numFailedCases > acceptableFailedCases) {
                doxygenStepFailed = true
                println "Doxygen failed to generate HTML documentation due to issued warnings displayed below."
                println "The total number of Doxygen warnings that needs to be corrected ( $numFailedCases ) is greater"
                println "than the acceptable number of warnings ( $acceptableFailedCases ). The warnings are the following:"
                for(line in failingCases) {
                    println line
                }
            }
            else{
                println "Doxygen successfully generated all the FileWriter HTML documentation without warnings."
            }
            archiveArtifacts "${test_output}"
            if(doxygenStepFailed) {
             unstable("Code is missing documentation. See log output for further information.")
            }
        }  // stage
}

def copy_binaries(builder, container) {
    builder.stage("${container.key}: Copying binaries") {
      def archive_output = "${builder.project}-${container.key}.tar.gz"
      container.sh """
        cd build
        rm -rf ${builder.project}; mkdir ${builder.project}
        mkdir ${builder.project}/bin
        cp ./bin/kafka-to-nexus ${builder.project}/bin/
        cp -r ./lib ${builder.project}/
        cp -r ./licenses ${builder.project}/

        cp ./CONAN_INFO ${builder.project}/

        # Create file with build information
        touch ${builder.project}/BUILD_INFO
        echo 'Repository: ${builder.project}/${env.BRANCH_NAME}' >> ${builder.project}/BUILD_INFO
        echo 'Commit: ${scm_vars.GIT_COMMIT}' >> ${builder.project}/BUILD_INFO
        echo 'Jenkins build: ${env.BUILD_NUMBER}' >> ${builder.project}/BUILD_INFO

        tar czf ${archive_output} ${builder.project}
      """

      container.copyFrom("build/${archive_output}", '.')
      container.copyFrom("build/${builder.project}/BUILD_INFO", '.')
    }
}

def archive(builder, container) {
    builder.stage("${container.key}: Archiving") {
      def archive_output = "${builder.project}-${container.key}.tar.gz"
      archiveArtifacts "${archive_output},BUILD_INFO"
    }
}

def integration_test(builder, container) {
    try {
      stage("${container.key}: Sys.-test requirements") {
        sh "tar xvf ${builder.project}-${container.key}.tar.gz"
        sh """cd kafka-to-nexus
       scl enable rh-python38 -- python -m pip install --user --upgrade pip
       scl enable rh-python38 -- python -m pip install --user -r integration-tests/requirements.txt
        """
      }  // stage
      dir("kafka-to-nexus/integration-tests") {
      stage("${container.key}: integration test run") {
        // Stop and remove any containers that may have been from the job before,
        // i.e. if a Jenkins job has been aborted.
        sh "docker stop \$(docker-compose ps -a -q) && docker rm \$(docker-compose ps -a -q) || true"
        timeout(time: 30, activity: true){
          sh """chmod go+w logs output-files
          LD_LIBRARY_PATH=../lib scl enable rh-python38 -- python -m pytest -s --writer-binary="../" --junitxml=./IntegrationTestsOutput.xml .
          """
        }
      }  // stage
      }
    } finally {
      stage ("${container.key}: Integration test clean-up") {
        dir("kafka-to-nexus/integration-tests") {
            // The statements below return true because the build should pass
            // even if there are no docker containers or output files to be
            // removed.
            sh """rm -rf output-files/* || true
            docker stop \$(docker-compose ps -a -q) && docker rm \$(docker-compose ps -a -q) || true
            """
            sh "chmod go-w logs output-files"
        }
      }  // stage
      stage("${container.key}: Integration test archive") {
        junit "kafka-to-nexus/integration-tests/IntegrationTestsOutput.xml"
        archiveArtifacts "kafka-to-nexus/integration-tests/logs/*.txt"
      }
    }
}

String almalinux_key = "almalinux8"
String ubuntu_key = "ubuntu2204"
String centos_key = "centos7"
String release_key = "centos7-release"
String integration_test_key = "integration-test"
String static_checks_key = "static-checks"

container_build_nodes = [
  (almalinux_key): ContainerBuildNode.getDefaultContainerBuildNode('almalinux8-gcc12'),
  (centos_key): ContainerBuildNode.getDefaultContainerBuildNode('centos7-gcc11'),
  (release_key): ContainerBuildNode.getDefaultContainerBuildNode('centos7-gcc11'),
  (ubuntu_key): ContainerBuildNode.getDefaultContainerBuildNode('ubuntu2204'),
  (static_checks_key): ContainerBuildNode.getDefaultContainerBuildNode('ubuntu2204')
]

base_steps = [{b,c -> checkout(b, c)}, {b,c -> cpp_dependencies(b, c)}]

container_build_node_steps = [
    (almalinux_key): base_steps + [{b,c -> configure(b, c, "", false)}, {b,c -> build(b, c, true)}, {b,c -> unit_tests(b, c, false)}],
    (centos_key): base_steps + [{b,c -> configure(b, c, "", false)}, {b,c -> build(b, c, true)}, {b,c -> unit_tests(b, c, false)}],
    (release_key): base_steps + [{b,c -> configure(b, c, "", true)}, {b,c -> build(b, c, false)}, {b,c -> copy_binaries(b, c)}, {b,c -> archive(b, c)}],
    (ubuntu_key): base_steps + [{b,c -> configure(b, c, "-DRUN_DOXYGEN=ON -DCOV=ON", false)}, {b,c -> build(b, c, true)}, {b,c -> unit_tests(b, c, true)}],
    (static_checks_key): base_steps + [{b,c -> configure(b, c, "-DRUN_DOXYGEN=ON", false)}, {b,c -> static_checks(b, c)}],
    (integration_test_key): base_steps + [{b,c -> configure(b, c, "", false)}, {b,c -> build(b, c, false)}, {b,c -> copy_binaries(b, c)}, {b,c -> integration_test(b, c)}]
]

if ( env.CHANGE_ID ) {
  container_build_nodes[integration_test_key] = ContainerBuildNode.getDefaultContainerBuildNode('centos7-gcc11')
}

pipeline_builder = new PipelineBuilder(this, container_build_nodes)
pipeline_builder.activateEmailFailureNotifications()

builders = pipeline_builder.createBuilders { container ->
    current_steps_list = container_build_node_steps[container.key]
    for (step in current_steps_list) {
        step(pipeline_builder, container)
    }
}  // createBuilders

node {
  dir("${project}") {
    try {
      scm_vars = checkout scm
    } catch (e) {
      failure_function(e, 'Checkout failed')
    }
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
