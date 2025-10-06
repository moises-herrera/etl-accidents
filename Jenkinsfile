pipeline {
    agent any
    
    environment {
        DOCKER_IMAGE = 'etl-accidents'
        DOCKER_TAG = 'latest'
        
        INPUT_DIR = "${WORKSPACE}/input"
        OUTPUT_DIR = "${WORKSPACE}/output"
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
                    sh "mkdir -p ${OUTPUT_DIR}"
                    
                    sh """
                        docker run --rm \
                            --name etl-run-${BUILD_NUMBER} \
                            -v ${INPUT_DIR}:/app/input:ro \
                            -v ${OUTPUT_DIR}:/app/output \
                            ${DOCKER_IMAGE}:${DOCKER_TAG} \
                            /bin/bash -c "python etl_accidents/etl.py --input-dir /app/input --output-dir /app/output"
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
                        echo "Archivos generados en ${OUTPUT_DIR}:"
                        ls -lh ${OUTPUT_DIR}
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
                    archiveArtifacts artifacts: 'output/**/*.parquet,output/**/*.png', 
                                     allowEmptyArchive: true,
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
            emailext(
                subject: "Pipeline exitoso: ${env.JOB_NAME} - Build #${env.BUILD_NUMBER}",
                body: """
                    <h2>Pipeline ejecutado exitosamente</h2>
                    <p><strong>Job:</strong> ${env.JOB_NAME}</p>
                    <p><strong>Build:</strong> ${env.BUILD_NUMBER}</p>
                    <p><strong>Status:</strong> SUCCESS</p>
                    <p><a href="${env.BUILD_URL}">Ver build</a></p>
                """,
                to: '${DEFAULT_RECIPIENTS}',
                mimeType: 'text/html'
            )
        }
        failure {
            echo 'Pipeline falló'
            emailext(
                subject: "Pipeline fallido: ${env.JOB_NAME} - Build #${env.BUILD_NUMBER}",
                body: """
                    <h2>Pipeline falló</h2>
                    <p><strong>Job:</strong> ${env.JOB_NAME}</p>
                    <p><strong>Build:</strong> ${env.BUILD_NUMBER}</p>
                    <p><strong>Status:</strong> FAILED</p>
                    <p><a href="${env.BUILD_URL}">Ver build</a></p>
                    <p><a href="${env.BUILD_URL}/console">Ver logs</a></p>
                """,
                to: '${DEFAULT_RECIPIENTS}',
                mimeType: 'text/html'
            )
        }
        unstable {
            echo 'Pipeline inestable'
        }
    }
}