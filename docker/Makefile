
DOCKER_REGISTRY = 106877218800.dkr.ecr.us-east-1.amazonaws.com
REPO_NAME = data-engineering/gha-runner/dag-factory
AIRFLOW_REPO = data-engineering/airflow
AIRFLOW_VERSION ?= 2.9.1
PYTHON_VERSION ?= 3.11
ACTIONS_RUNNER_VERSION ?= v2.304.0
build:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 106877218800.dkr.ecr.us-east-1.amazonaws.com
	docker build --platform=linux/amd64 --no-cache -f ./Dockerfile --pull  \
				 --build-arg AIRFLOW_IMAGE="${DOCKER_REGISTRY}/${AIRFLOW_REPO}:${AIRFLOW_VERSION}-python${PYTHON_VERSION}" \
				 --build-arg ACTIONS_RUNNER_VERSION=${ACTIONS_RUNNER_VERSION} \
				 -t "${DOCKER_REGISTRY}/${REPO_NAME}:${AIRFLOW_VERSION}-python${PYTHON_VERSION}--dag-factory-runner" \
				 ../
push:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 106877218800.dkr.ecr.us-east-1.amazonaws.com
	docker push "${DOCKER_REGISTRY}/${REPO_NAME}:${AIRFLOW_VERSION}-python${PYTHON_VERSION}--dag-factory-runner"
	docker tag "${DOCKER_REGISTRY}/${REPO_NAME}:${AIRFLOW_VERSION}-python${PYTHON_VERSION}--dag-factory-runner" "${DOCKER_REGISTRY}/${REPO_NAME}:prod"
	docker push "${DOCKER_REGISTRY}/${REPO_NAME}:prod"

push-dev:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 106877218800.dkr.ecr.us-east-1.amazonaws.com
	# docker push "${DOCKER_REGISTRY}/${REPO_NAME}:${AIRFLOW_VERSION}-python${PYTHON_VERSION}--dag-factory-runner"
	docker tag "${DOCKER_REGISTRY}/${REPO_NAME}:${AIRFLOW_VERSION}-python${PYTHON_VERSION}--dag-factory-runner" "${DOCKER_REGISTRY}/${REPO_NAME}:dev"
	docker push "${DOCKER_REGISTRY}/${REPO_NAME}:dev"