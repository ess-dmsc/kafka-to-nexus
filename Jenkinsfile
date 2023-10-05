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

builders = pipeline_builder.createBuilders { container ->
  pipeline_builder.stage("${container.key}: checkout") {
    dir(pipeline_builder.project) {
      scm_vars = checkout scm
    }
    // Copy source code to container
    container.copyTo(pipeline_builder.project, pipeline_builder.project)
  }  // stage: checkout

  pipeline_builder.stage("${container.key}: dependencies") {
    container.sh """
      mkdir build
      cd build
      conan install --build=outdated ../${pipeline_builder.project}/conanfile.txt
      conan info ../${pipeline_builder.project}/conanfile.txt > CONAN_INFO
    """
  }  // stage: dependencies

  pipeline_builder.stage("${container.key}: configuration") {
    if (container.key == release_node) {
      container.sh """
        cd build
        ../${pipeline_builder.project}/jenkins-scripts/configure-release.sh \
          ../${pipeline_builder.project}
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

  pipeline_builder.stage("${container.key}: build") {
    container.sh """
      cd build
      ninja kafka-to-nexus UnitTests
    """
  }  // stage: build

  pipeline_builder.stage("${container.key}: test") {
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

  pipeline_builder.stage("${container.key}: documentation") {
    container.sh """
      cd build
      ninja docs
    """
  }  // stage: documentation

  if (container.key == release_os) {
    def archive_output = "${builder.project}-${container.key}.tar.gz"
    pipeline_builder.stage("${container.key}: archive") {
      // Create archive file
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

      // Copy files from container and archive
      container.copyFrom("build/${archive_output}", '.')
      container.copyFrom("build/${builder.project}/BUILD_INFO", '.')
      archiveArtifacts "${archive_output},BUILD_INFO"
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
    pr_pipeline_builder.stage("${container.key}: checkout") {
      dir(pr_pipeline_builder.project) {
        scm_vars = checkout scm
      }
      // Copy source code to container
      container.copyTo(pr_pipeline_builder.project, pr_pipeline_builder.project)
    }  // stage: checkout

    pr_pipeline_builder.stage("${container.key}: clang-format") {
      container.sh """
        cd ${pr_pipeline_builder.project}
        jenkins-scripts/check-formatting.sh
      """
    }  // stage: clang-format 

    pr_pipeline_builder.stage("${container.key}: black") {
      container.sh """
        cd ${pr_pipeline_builder.project}
        python3 -m black --version
        python3 -m black --check integration-tests
      """
    }  // stage: black

    pr_pipeline_builder.stage("${container.key}: cppcheck") {
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
      container.copyFrom("${pr_pipeline_builder.project}/cppcheck.xml", pr_pipeline_builder.project)
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
    parallel builders
  } catch (e) {
    pipeline_builder.handleFailureMessages()
    throw e
  }

  // Delete workspace when build is done
  cleanWs()
}
