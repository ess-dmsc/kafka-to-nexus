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

container_build_nodes = [
  'almalinux8': ContainerBuildNode.getDefaultContainerBuildNode('almalinux8-gcc12'),
  'centos7': ContainerBuildNode.getDefaultContainerBuildNode('centos7-gcc11'),
  'centos7-release': ContainerBuildNode.getDefaultContainerBuildNode('centos7-gcc11'),
  'ubuntu2204': ContainerBuildNode.getDefaultContainerBuildNode('ubuntu2204')
]

if (env.CHANGE_ID) {
  container_build_nodes['static-checks'] = ContainerBuildNode.getDefaultContainerBuildNode('ubuntu2204')
}

pipeline_builder = new PipelineBuilder(this, container_build_nodes)
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

  // Only run static checks in pull requests
  if (env.CHANGE_ID && container.key == 'ubuntu2204') {

    pipeline_builder.stage("${container.key}: clang-format") {
      container.sh """
        cd ${pipeline_builder.project}
        jenkins-scripts/check-formatting.sh
      """
    }  // stage: clang-format 

    pipeline_builder.stage("${container.key}: black") {
      container.sh """
        cd ${pipeline_builder.project}
        python -m black --version
        python -m black --check integration-tests
      """
    }  // stage: clang-format 

  }  // if

}  // createBuilders

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
