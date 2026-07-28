// Jenkinsfile (Declarative Pipeline)

pipeline {
    agent any

    options {
        skipDefaultCheckout(true)
        disableConcurrentBuilds()
        timeout(time: 45, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '20'))
        timestamps()
    }

    environment {
        HARBOR = '10.17.158.118'
        IMAGE_REPOSITORY = '10.17.158.118/storagent/storagent_backend'
        SOURCE_URL = 'https://github.com/zl875136491/storagent'
        DOCKERFILE_PATH = 'Dockerfile'
        TEST_IMAGE = 'python:3.12.10-bookworm'
        PYPI_INDEX_URL = 'https://pypi.tuna.tsinghua.edu.cn/simple'
    }

    stages {
        stage('Checkout') {
            steps {
                deleteDir()
                script {
                    def scmVars = checkout(scm) ?: [:]

                    env.GIT_COMMIT = scmVars.GIT_COMMIT ?: sh(
                        script: 'git rev-parse HEAD',
                        returnStdout: true
                    ).trim()

                    if (!(env.GIT_COMMIT ==~ /[0-9a-fA-F]{40}/)) {
                        error("Unable to determine a full Git commit SHA: '${env.GIT_COMMIT}'.")
                    }

                    if (env.BRANCH_NAME == 'master') {
                        env.IMAGE_TAG = "sha-${env.GIT_COMMIT.take(12).toLowerCase()}"
                        env.IMAGE_REF = "${env.IMAGE_REPOSITORY}:${env.IMAGE_TAG}"
                        currentBuild.displayName = env.IMAGE_TAG
                    }
                }
            }
        }

        stage('Test') {
            steps {
                sh '''#!/usr/bin/env bash
                    set -Eeuo pipefail
                    docker run --rm \
                        --platform linux/amd64 \
                        --volume "$WORKSPACE:/workspace:ro" \
                        --workdir /workspace \
                        --env PYTHONDONTWRITEBYTECODE=1 \
                        --env PIP_DISABLE_PIP_VERSION_CHECK=1 \
                        --env "PIP_INDEX_URL=$PYPI_INDEX_URL" \
                        --env LOG_PATH=/tmp/storagent-logs \
                        "$TEST_IMAGE" \
                        bash -c '
                            set -Eeuo pipefail
                            python -m pip install --no-cache-dir -r requirements-dev.txt
                            python -m pytest -q -p no:cacheprovider
                        '
                '''
            }
        }

        stage('Build Docker Image') {
            when {
                expression { env.BRANCH_NAME == 'master' }
            }
            steps {
                sh '''#!/usr/bin/env bash
                    set -Eeuo pipefail
                    docker build \
                        --provenance=false \
                        --platform linux/amd64 \
                        --file "$DOCKERFILE_PATH" \
                        --build-arg "VCS_REF=$GIT_COMMIT" \
                        --build-arg "IMAGE_VERSION=$IMAGE_TAG" \
                        --build-arg "SOURCE_URL=$SOURCE_URL" \
                        --tag "$IMAGE_REF" \
                        .

                    test "$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$IMAGE_REF")" = "$GIT_COMMIT"
                    test "$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.component" }}' "$IMAGE_REF")" = 'backend'
                '''
            }
        }

        stage('Push Docker Image') {
            when {
                expression { env.BRANCH_NAME == 'master' }
            }
            steps {
                withCredentials([usernamePassword(
                    credentialsId: 'infra_harbor_auth',
                    usernameVariable: 'INFRA_HARBOR_USR',
                    passwordVariable: 'INFRA_HARBOR_PSW'
                )]) {
                    sh '''#!/usr/bin/env bash
                        set -Eeuo pipefail
                        set +x

                        DOCKER_CONFIG="$(mktemp -d "${WORKSPACE_TMP:-/tmp}/storagent-docker-config.XXXXXX")"
                        export DOCKER_CONFIG
                        PUSH_LOG="$(mktemp "${WORKSPACE_TMP:-/tmp}/storagent-docker-push.XXXXXX")"

                        cleanup_registry_session() {
                            docker logout "$HARBOR" >/dev/null 2>&1 || true
                            rm -rf "$DOCKER_CONFIG"
                            rm -f "$PUSH_LOG"
                        }
                        trap cleanup_registry_session EXIT HUP INT TERM

                        printf '%s' "$INFRA_HARBOR_PSW" | \
                            docker login "$HARBOR" --username "$INFRA_HARBOR_USR" --password-stdin >/dev/null
                        unset INFRA_HARBOR_USR INFRA_HARBOR_PSW

                        docker push "$IMAGE_REF" 2>&1 | tee "$PUSH_LOG"
                        IMAGE_DIGEST="$(awk '/digest: sha256:/{for (i = 1; i <= NF; i++) if ($i ~ /^sha256:/) {print $i; exit}}' "$PUSH_LOG")"

                        if ! printf '%s\n' "$IMAGE_DIGEST" | grep -Eq '^sha256:[0-9a-f]{64}$'; then
                            echo 'Docker push completed without a valid registry digest.' >&2
                            exit 1
                        fi

                        printf '%s\n' \
                            'component=backend' \
                            "repository=$IMAGE_REPOSITORY" \
                            "tag=$IMAGE_TAG" \
                            "digest=$IMAGE_DIGEST" \
                            "reference=$IMAGE_REPOSITORY@$IMAGE_DIGEST" \
                            "gitCommit=$GIT_COMMIT" \
                            "buildNumber=$BUILD_NUMBER" \
                            "buildUrl=${BUILD_URL:-}" \
                            > backend-image.properties
                    '''
                }

                archiveArtifacts(
                    artifacts: 'backend-image.properties',
                    fingerprint: true,
                    onlyIfSuccessful: true
                )
            }
        }
    }

    post {
        always {
            sh '''#!/usr/bin/env bash
                set +e
                if [ -n "${IMAGE_REF:-}" ]; then
                    docker image rm "$IMAGE_REF" >/dev/null 2>&1 || true
                fi
            '''
            deleteDir()
        }
        success {
            script {
                if (env.BRANCH_NAME == 'master') {
                    echo "Backend image published as ${env.IMAGE_REF}."
                } else {
                    echo "Backend tests passed for ${env.BRANCH_NAME ?: 'an unclassified branch'}; image publishing was skipped."
                }
            }
        }
        failure {
            echo 'Backend image pipeline failed.'
        }
    }
}
