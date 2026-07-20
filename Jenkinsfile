// Jenkinsfile (Declarative Pipeline)

pipeline {
    agent any

    environment {
        // 构建的 Docker 镜像名称
        DOCKER_IMAGE = "storagent_backend"
        // 镜像的标签，使用构建号作为唯一标识
        DOCKER_TAG = "${env.BUILD_NUMBER}"
        // 应用源码目录（Dockerfile 所在路径）
        // APP_DIR = "storagent"
        // Dockerfile 文件名
        DOCKERFILE_PATH = "Dockerfile"
        // Harbor 地址
        HARBOR = "10.17.151.187"
        // Harbor 仓库
        HARBOR_REPO = "storagent"
    }

    stages {
        stage('Checkout') {
            steps {
                echo 'skip scm test'
            }
        }

        stage('Test') {
            steps {
                script {
                    sh '''
                      set -e
                      python3 -m venv .venv || true
                      . .venv/bin/activate
                      pip install -q -r requirements-dev.txt
                      pytest -q
                    '''
                }
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    // dir("${APP_DIR}") {
                        sh "docker build -f ${DOCKERFILE_PATH} -t ${DOCKER_IMAGE}:${DOCKER_TAG} ."
                        sh "docker tag ${DOCKER_IMAGE}:${DOCKER_TAG} ${HARBOR}/${HARBOR_REPO}/${DOCKER_IMAGE}:${DOCKER_TAG}"
                    //}
                }
            }
        }

        stage('Push Docker Image') {
            steps {
                script {
                    withCredentials([usernamePassword(credentialsId: 'infra_harbor_auth', usernameVariable: 'INFRA_HARBOR_USR', passwordVariable: 'INFRA_HARBOR_PSW')]) {
                        sh "echo ${INFRA_HARBOR_PSW} | docker login ${HARBOR} -u ${INFRA_HARBOR_USR} --password-stdin"
                    }

                    sh "docker push ${HARBOR}/${HARBOR_REPO}/${DOCKER_IMAGE}:${DOCKER_TAG}"
                }
            }
        }
    }

    post {
        always {
            sh "docker rmi ${DOCKER_IMAGE}:${DOCKER_TAG} || true"
            sh "docker rmi ${HARBOR}/${HARBOR_REPO}/${DOCKER_IMAGE}:${DOCKER_TAG} || true"
            cleanWs()
        }
        success {
            echo 'Pipeline successfully finished!'
        }
        failure {
            echo 'Pipeline failed!'
        }
    }
}
