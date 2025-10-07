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
                        mkdir -p ./test-results ./coverage
                        chmod 777 ./test-results ./coverage
                    """

                    sh """
                        docker run \
                            --name etl-tests-${BUILD_NUMBER} \
                            -v \$(pwd)/test-results:/app/test-results \
                            -v \$(pwd)/coverage:/app/coverage \
                            ${DOCKER_IMAGE}:${DOCKER_TAG} \
                            /bin/bash -c "python -m pytest tests/ -v --tb=short \
                                --junitxml=/app/test-results/junit.xml \
                                --cov=etl_accidents \
                                --cov-report=html:/app/coverage/html \
                                --cov-report=xml:/app/coverage/coverage.xml \
                                --cov-report=term"

                        docker cp etl-tests-${BUILD_NUMBER}:/app/coverage/. ./coverage/
                        docker cp etl-tests-${BUILD_NUMBER}:/app/test-results/. ./test-results/
                        docker rm -f etl-tests-${BUILD_NUMBER}
                    """
                }
            }
            post {
                always {
                    echo 'Tests completados'
                    
                    // Publicar resultados de tests JUnit
                    junit allowEmptyResults: true, testResults: 'test-results/junit.xml'
                    
                    // Archivar reportes de cobertura
                    archiveArtifacts artifacts: 'test-results/**/*.xml,coverage/**/*', 
                                     allowEmptyArchive: true,
                                     fingerprint: true
                    
                    // Publicar reporte de cobertura HTML
                    publishHTML([
                        allowMissing: true,
                        alwaysLinkToLastBuild: true,
                        keepAll: true,
                        reportDir: 'coverage/html',
                        reportFiles: 'index.html',
                        reportName: 'Coverage Report',
                        reportTitles: 'Code Coverage'
                    ])
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
                            /bin/bash -c "python etl_accidents/etl.py --input-dir /app/input --output-dir /app/output && python scripts/generate_charts.py"
                        
                        docker cp etl-run-${BUILD_NUMBER}:/app/output/. ./output/
                        
                        docker rm -f etl-run-${BUILD_NUMBER}
                    """
                }
            }
            post {
                always {
                    echo 'ETL finalizado'
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

                    echo 'Artifacts guardados exitosamente'
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