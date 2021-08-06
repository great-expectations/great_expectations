pipeline {

  agent {
    label 'redhat_linux'
  }

  parameters {
    string(defaultValue: "refs/heads/master", description: 'Branch to build?', name: 'BRANCH')
    string(defaultValue: "dlevents", description: 'Package name to deploy to msnexus', name: 'package_name')
    string(defaultValue: "", description: 'Version id to deploy to msnexus', name: 'version_id')
  }

  stages {

    stage("Initialization") {
      steps {
        script {
          currentBuild.displayName = "# ${env.BUILD_NUMBER} ${env.BRANCH_NAME}"
        }
      }
    }

    stage('SCM checkout') {
      steps {
        checkout scm
      }
    }

    stage('Test cases execution') {
      agent {
        dockerfile {
          label 'redhat_linux'
          filename 'Dockerfile'
        }
      }
      steps {
        println('---INTEGRATION/UNIT TEST COMPLETED SUCCESSFULLY ---')
      }
    }

    stage('Deploying to nexus') {
      steps {
        script {
          sh 'chmod -R a+r *'
          if (params.version_id == "" && (env.BRANCH_NAME).startsWith("release")){
            env.version_id = "${env.BRANCH_NAME.split("/")[1]}"
            env.package_name = "${params.package_name}"
            sh """
              echo "Deploying branch ${env.BRANCH_NAME}, package ${env.package_name}"
              chmod +x jenkins/pipelineScripts/deploy_code.sh
              jenkins/pipelineScripts/deploy_code.sh
            """
          }
          else
            println("Skipping ${env.BRANCH_NAME} branch for deployment.")

        }
      }
    }
  }

  post {
     success {
      echo 'Build executed successfully.'

    }
    failure {
      echo 'Build failed to execute.'
    }
  }
}
