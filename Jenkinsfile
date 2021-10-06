@Library('ecdc-pipeline')
import ecdcpipeline.ContainerBuildNode
import ecdcpipeline.PipelineBuilder

project = "kafka-to-nexus"

clangformat_os = "ubuntu2004"
test_and_coverage_os = "ubuntu2004"
release_os = "centos7-release"

container_build_nodes = [
  'centos7-release': ContainerBuildNode.getDefaultContainerBuildNode('centos7-gcc8'),
  'ubuntu2004': ContainerBuildNode.getDefaultContainerBuildNode('ubuntu2004')
]

// Define number of old builds to keep. These numbers are somewhat arbitrary,
// but based on the fact that for the master branch we want to have a certain
// number of old builds available, while for the other branches we want to be
// able to deploy easily without using too much disk space.
def num_artifacts_to_keep
if (env.BRANCH_NAME == 'master') {
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
    if (container.key == "alpine") {
      // Dirty hack to override flatbuffers/1.11.0 with 1.10 as a bug means 1.11 doesn't build on alpine
      // Fixed at HEAD, so can be removed when flatbuffers 1.12 is released
      // Explicit build of boost_build required because otherwise we get a version of b2 built against glibc (alpine instead has musl)
      container.sh """
        sed -i '10iflatbuffers/1.10.0@google/stable' ${pipeline_builder.project}/conanfile.txt
        conan install "boost_build/1.69.0@bincrafters/stable" --build
      """
    }

    def conan_remote = "ess-dmsc-local"
    container.sh """
      mkdir build
      cd build
      conan remote add \
        --insert 0 \
        ${conan_remote} ${local_conan_server}
      conan install --build=outdated ../${pipeline_builder.project}/conanfile.txt
    """
  }  // stage

  pipeline_builder.stage("${container.key}: Configure") {
    if (container.key != release_os) {
      def coverage_on
      if (container.key == test_and_coverage_os) {
        coverage_on = '-DCOV=ON'
      } else {
        coverage_on = ''
      }

      def doxygen_on
      if (container.key == clangformat_os) {
        doxygen_on = '-DRUN_DOXYGEN=ON'
      } else {
        doxygen_on = ''
      }

      container.sh """
        cd build
        . ./activate_run.sh
        cmake ${coverage_on} ${doxygen_on} -GNinja ../${pipeline_builder.project}
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
          -DBUILD_TESTS=FALSE \
          ../${pipeline_builder.project}
      """
    }
  }  // stage

  pipeline_builder.stage("${container.key}: Build") {
    container.sh """
    cd build
    . ./activate_run.sh
    ninja all
    """
  }  // stage

  pipeline_builder.stage("${container.key}: Test") {
    // env.CHANGE_ID is set for pull request builds.
    if (container.key == test_and_coverage_os) {
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

    } else if (container.key != release_os) {
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

      container.setupLocalGitUser(pipeline_builder.project)

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
          /home/jenkins/.local/bin/black system-tests
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
      try {
        withCredentials([
          usernamePassword(
          credentialsId: 'cow-bot-username-with-token',
          usernameVariable: 'USERNAME',
          passwordVariable: 'PASSWORD'
          )
        ]) {
          withEnv(["PROJECT=${pipeline_builder.project}"]) {
            container.sh '''
              cd $PROJECT
              git push https://$USERNAME:$PASSWORD@github.com/ess-dmsc/$PROJECT.git HEAD:$CHANGE_BRANCH
            '''
          }  // withEnv
        }  // withCredentials
      } catch (e) {
        // Okay to fail; there may be nothing to push
      } finally {
        // Clean up
      }
    }  // stage

    pipeline_builder.stage("${container.key}: Cppcheck") {
      def test_output = "cppcheck.xml"
        container.sh """
          cd ${pipeline_builder.project}
          cppcheck --xml --inline-suppr --suppress=unusedFunction --suppress=missingInclude --enable=all --inconclusive src/ 2> ${test_output}
        """
        container.copyFrom("${pipeline_builder.project}/${test_output}", '.')
        recordIssues sourceCodeEncoding: 'UTF-8', qualityGates: [[threshold: 1, type: 'TOTAL', unstable: true]], tools: [cppCheck(pattern: 'cppcheck.xml', reportEncoding: 'UTF-8')]
    }  // stage

    pipeline_builder.stage("${container.key}: Doxygen") {
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

        int numFailedCases = failingCases.size()
        if(numFailedCases) {
            doxygenStepFailed = true
            println "Doxygen failed to generate HTML documentation due to issued warnings displayed below."
            println "The total number of Doxygen warnings that needs to be corrected is $numFailedCases and are following:"
            for(line in failingCases) {
                println line
            }
        }
        else{
            println "Doxygen successfully generated all the FileWriter HTML documentation without warnings."
        }
        archiveArtifacts "${test_output}"
        if(doxygenStepFailed) {
            // Error due to missing documentation is disabled for now.
//          error("Doxygen step failed. See log output for further information.")
        }
    }  // stage
  }  // if

  if (container.key == release_os) {
    pipeline_builder.stage("${container.key}: Archiving") {
      def archive_output = "${pipeline_builder.project}-${container.key}.tar.gz"
      container.sh """
        cd build
        rm -rf ${pipeline_builder.project}; mkdir ${pipeline_builder.project}
        mkdir ${pipeline_builder.project}/bin
        cp ./bin/kafka-to-nexus ${pipeline_builder.project}/bin/
        cp -r ./lib ${pipeline_builder.project}/
        cp -r ./licenses ${pipeline_builder.project}/

        # Create file with build information
        touch ${pipeline_builder.project}/BUILD_INFO
        echo 'Repository: ${pipeline_builder.project}/${env.BRANCH_NAME}' >> ${pipeline_builder.project}/BUILD_INFO
        echo 'Commit: ${scm_vars.GIT_COMMIT}' >> ${pipeline_builder.project}/BUILD_INFO
        echo 'Jenkins build: ${env.BUILD_NUMBER}' >> ${pipeline_builder.project}/BUILD_INFO

        tar czf ${archive_output} ${pipeline_builder.project}
      """

      container.copyFrom("build/${archive_output}", '.')
      container.copyFrom("build/${pipeline_builder.project}/BUILD_INFO", '.')
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
            sh "make -j4 all UnitTests"
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
              chmod go+w logs output-files
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
              sh "cd system-tests && chmod go-w logs output-files"
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
