pipeline {
    agent any

    environment {
        DOCKER_IMAGE = 'etl-accidents'
        DOCKER_TAG = 'latest'
    }

    stages {
        stage('Build') {
            steps {
                script {
                    sh """
                        echo "Building Docker image: ${DOCKER_IMAGE}:${DOCKER_TAG}"
                        docker build -t ${DOCKER_IMAGE}:${DOCKER_TAG} .
                    """

                    // Verificar que la imagen se creó correctamente
                    sh "docker images | grep ${DOCKER_IMAGE}"
                }
            }
        }

        stage('Tests') {
            steps {
                script {
                    sh """
                        docker run --rm \
                            --name etl-tests-${BUILD_NUMBER} \
                            ${DOCKER_IMAGE}:${DOCKER_TAG} \
                            /bin/bash -c "python -m pytest tests/ -v --tb=short"
                    """
                }
            }
            post {
                always {
                    echo 'Tests completados'
                }
                success {
                    echo 'Todos los tests pasaron exitosamente'
                }
                failure {
                    echo 'Algunos tests fallaron'
                }
            }
        }

        stage('Run ETL') {
            steps {
                script {
                    sh """
                        mkdir -p ./input ./output
                        chmod 777 ./output
                    """

                    sh """
                        docker run \
                            --name etl-run-${BUILD_NUMBER} \
                            -v \$(pwd)/input:/app/input:ro \
                            -v \$(pwd)/output:/app/output \
                            ${DOCKER_IMAGE}:${DOCKER_TAG} \
                            /bin/bash -c "python etl_accidents/etl.py --input-dir /app/input --output-dir /app/output"
                        
                        docker cp etl-run-${BUILD_NUMBER}:/app/output/. ./output/
                        
                        docker rm -f etl-run-${BUILD_NUMBER}
                    """
                }
            }
            post {
                always {
                    echo 'ETL finalizado'

                    sh """
                        echo "Contenido de output/"
                        ls -lR ./output
                    """
                }
                success {
                    echo 'ETL ejecutado exitosamente'

                    sh """
                        echo "Archivos generados en output/:"
                        ls -lh ./output
                    """
                }
                failure {
                    echo 'ETL falló durante la ejecución'
                }
            }
        }

        stage('Archive Artifacts') {
            steps {
                script {
                    archiveArtifacts artifacts: 'output/**/*', 
                                     allowEmptyArchive: false,
                                     fingerprint: true,
                                     onlyIfSuccessful: true

                    echo 'Artifacts archivados exitosamente'
                }
            }
        }
    }

    post {
        always {
            echo 'Pipeline completado'
            script {
                sh '''
                    # Limpiar contenedores detenidos
                    docker container prune -f
                '''
            }
        }
        success {
            echo 'Pipeline ejecutado exitosamente'
        }
        failure {
            echo 'Pipeline falló'
        }
        unstable {
            echo 'Pipeline inestable'
        }
    }
}