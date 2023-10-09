@Library('ecdc-pipeline')
import ecdcpipeline.ContainerBuildNode
import ecdcpipeline.PipelineBuilder

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

// Set number of old builds to keep
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

// Define node labels for additional steps
def release_node = 'centos7-release'  // Build for archiving artefact
def coverage_node = 'ubuntu2204'  // Calculate test coverage

build_nodes = [
  'almalinux8': ContainerBuildNode.getDefaultContainerBuildNode('almalinux8-gcc12'),
  'centos7': ContainerBuildNode.getDefaultContainerBuildNode('centos7-gcc11'),
  (release_node): ContainerBuildNode.getDefaultContainerBuildNode('centos7-gcc11'),
  (coverage_node): ContainerBuildNode.getDefaultContainerBuildNode('ubuntu2204')
]

pipeline_builder = new PipelineBuilder(this, build_nodes)
pipeline_builder.activateEmailFailureNotifications()

// Define name for the archive file used in main pipeline and integration test
def archive_output = "${pipeline_builder.project}-${release_node}.tar.gz"

// Main build and test pipeline
builders = pipeline_builder.createBuilders { container ->
  pipeline_builder.stage("${container.key}: Checkout") {
    dir(pipeline_builder.project) {
      scm_vars = checkout scm
    }
    // Copy source code to container
    container.copyTo(pipeline_builder.project, pipeline_builder.project)
  }  // stage: checkout

  pipeline_builder.stage("${container.key}: Dependencies") {
    container.sh """
      mkdir build
      cd build
      conan install --build=outdated ../${pipeline_builder.project}/conanfile.txt
      conan info ../${pipeline_builder.project}/conanfile.txt > CONAN_INFO
    """
  }  // stage: dependencies

  pipeline_builder.stage("${container.key}: Configuration") {
    if (container.key == release_node) {
      container.sh """
        ${pipeline_builder.project}/jenkins-scripts/configure-release.sh \
          ${pipeline_builder.project} \
          build
      """
    } else if (container.key == coverage_node) {
      container.sh """
        cd build
        cmake -DCOV=ON -DRUN_DOXYGEN=ON -GNinja ../${pipeline_builder.project}
      """
    } else {
      container.sh """
        cd build
        cmake -DRUN_DOXYGEN=ON -GNinja ../${pipeline_builder.project}
      """
    }
  }  // stage: configuration

  pipeline_builder.stage("${container.key}: Build") {
    container.sh """
      cd build
      ninja kafka-to-nexus UnitTests
    """
  }  // stage: build

  pipeline_builder.stage("${container.key}: Test") {
    if (container.key == coverage_node) {
      container.sh """
        cd build
        ./bin/UnitTests -- --gtest_output=xml:test_results.xml
        ninja coverage
      """

      // Copy test and coverage results
      container.copyFrom('build', '.')

      // Publish test results
      junit "build/test_results.xml"

      // Publish test coverage
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
    } else {
      // Not a coverage node
      container.sh """
        cd build
        ./bin/UnitTests
      """
    }
  }  // stage: test

  pipeline_builder.stage("${container.key}: Documentation") {
    container.sh """
      cd build
      ninja docs
    """
  }  // stage: documentation

  if (container.key == release_node) {
    pipeline_builder.stage("${container.key}: Archive") {
      // Create archive file
      container.sh """
        cd build
        rm -rf ${pipeline_builder.project}; mkdir ${pipeline_builder.project}
        mkdir ${pipeline_builder.project}/bin
        cp ./bin/kafka-to-nexus ${pipeline_builder.project}/bin/
        cp -r ./lib ${pipeline_builder.project}/
        cp -r ./licenses ${pipeline_builder.project}/

        cp ./CONAN_INFO ${pipeline_builder.project}/

        # Create file with build information
        touch ${pipeline_builder.project}/BUILD_INFO
        echo 'Repository: ${pipeline_builder.project}/${env.BRANCH_NAME}' \
          >> ${pipeline_builder.project}/BUILD_INFO
        echo 'Commit: ${scm_vars.GIT_COMMIT}' >> ${pipeline_builder.project}/BUILD_INFO
        echo 'Jenkins build: ${env.BUILD_NUMBER}' >> ${pipeline_builder.project}/BUILD_INFO

        tar czf ${archive_output} ${pipeline_builder.project}
      """

      // Copy files from container and archive
      container.copyFrom("build/${archive_output}", '.')
      container.copyFrom("build/${pipeline_builder.project}/BUILD_INFO", '.')
      archiveArtifacts "${archive_output},BUILD_INFO"

      // Stash archive file for integration test in pull request builds
      if (env.CHANGE_ID) {
        stash "${archive_output}"
      }
    }  // stage: archive
  }  // if

}  // createBuilders

// Only run static checks in pull requests
if (env.CHANGE_ID) {
  pr_checks_nodes = [
    'pr-checks': ContainerBuildNode.getDefaultContainerBuildNode('ubuntu2204')
  ]

  pr_pipeline_builder = new PipelineBuilder(this, pr_checks_nodes)
  pr_pipeline_builder.activateEmailFailureNotifications()

  pr_checks_builders = pr_pipeline_builder.createBuilders { container ->
    pr_pipeline_builder.stage("${container.key}: Checkout") {
      dir(pr_pipeline_builder.project) {
        scm_vars = checkout scm
      }
      // Copy source code to container
      container.copyTo(pr_pipeline_builder.project, pr_pipeline_builder.project)
    }  // stage: checkout

    pr_pipeline_builder.stage("${container.key}: Clang-format") {
      container.sh """
        cd ${pr_pipeline_builder.project}
        jenkins-scripts/check-formatting.sh
      """
    }  // stage: clang-format 

    pr_pipeline_builder.stage("${container.key}: Black") {
      container.sh """
        cd ${pr_pipeline_builder.project}
        python3 -m black --version
        python3 -m black --check integration-tests
      """
    }  // stage: black

    pr_pipeline_builder.stage("${container.key}: Cppcheck") {
      container.sh """
        cd ${pr_pipeline_builder.project}
        cppcheck --version
        cppcheck \
          --xml \
          --inline-suppr \
          --suppress=unusedFunction \
          --suppress=missingInclude \
          --enable=all \
          --inconclusive \
          src/ 2> cppcheck.xml
      """

      // Copy files from container and publish report
      container.copyFrom(
        "${pr_pipeline_builder.project}/cppcheck.xml",
        pr_pipeline_builder.project
      )
      dir("${pr_pipeline_builder.project}") {
        recordIssues \
          quiet: true,
          sourceCodeEncoding: 'UTF-8',
          qualityGates: [[
            threshold: 1,
            type: 'TOTAL',
            unstable: true
          ]],
          tools: [cppCheck(pattern: 'cppcheck.xml', reportEncoding: 'UTF-8')]
      }  // dir
    }  // stage: cppcheck
  }  // PR checks createBuilders

  builders = builders + pr_checks_builders
}  // if

node('master') {
  dir("${pipeline_builder.project}") {
    scm_vars = checkout scm
  }

  try {
    // Start pipelines
    parallel builders
  } catch (e) {
    pipeline_builder.handleFailureMessages()
    throw e
  }

  // Delete workspace when build is done
  cleanWs()
}

// Only run integration test on pull requests
if (env.CHANGE_ID) {
  node('docker') {
    stage('checkout') {
      checkout scm
      unstash "${archive_output}"
      sh "tar xvf ${archive_output}"
    }  // stage: checkout

    stage("requirements") {
      dir("integration-tests") {
        sh """
          scl enable rh-python38 -- python -m venv venv
          venv/bin/pip install --upgrade pip
          venv/bin/pip install -r requirements.txt
        """
      }  // dir
    }  // stage: requirements

    try {
      dir("integration-tests") {
        stage("integration-test") {
          // Stop and remove any containers that may have been from the job before,
          // i.e. if a Jenkins job has been aborted.
          sh """
            docker stop \$(docker-compose ps -a -q) \
              && docker rm \$(docker-compose ps -a -q) \
              || true
          """

          // Limit run to 30 minutes
          timeout(time: 30, activity: true) {
            sh """
              chmod go+w logs output-files
              LD_LIBRARY_PATH=../lib venv/bin/python -m pytest \
                -s \
                --writer-binary="../kafka-to-nexus" \
                --junitxml=./IntegrationTestsOutput.xml \
                .
            """
          }  // timeout
        }  // stage: integration-test
      }  // dir
    } finally {
      dir("integration-tests") {
        stage ("clean-up") {
          // The statements below return true because the build should pass
          // even if there are no docker containers or output files to be
          // removed.
          sh """
            rm -rf output-files/* || true
            docker stop \$(docker-compose ps -a -q) \
              && docker rm \$(docker-compose ps -a -q) \
              || true
            chmod go-w logs output-files
          """
        }  // stage: clean-up

        stage("results") {
          junit "IntegrationTestsOutput.xml"
          archiveArtifacts "logs/*.txt"
        }  // stage: results
      }  // dir
    }  // try/finally
  }  // node
}  // if
