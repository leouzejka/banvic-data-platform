ifneq (,$(wildcard .env))
include .env
export
endif

KIND_CLUSTER := banvic
KIND_CONTEXT := kind-$(KIND_CLUSTER)

.PHONY: deps run kind-check airflow-image terraform-apply postgres-wait metadata-manifest metadata-init airflow airflow-manifests airflow-apply migration-wait airflow-wait scheduler-wait dag-processor-wait

run: deps kind-check airflow-image terraform-apply postgres-wait metadata-init airflow-apply migration-wait airflow-wait scheduler-wait dag-processor-wait
	@echo "Cluster Kubernetes e infraestrutura prontos."

deps:
	@echo "Verificando dependências..."
	@command -v docker >/dev/null || (echo "Docker não encontrado." && exit 1)
	@command -v kind >/dev/null || (echo "Kind não encontrado." && exit 1)
	@command -v kubectl >/dev/null || (echo "kubectl não encontrado." && exit 1)
	@command -v terraform >/dev/null || (echo "Terraform não encontrado." && exit 1)
	@command -v make >/dev/null || (echo "Make não encontrado." && exit 1)
	@echo "Todas as dependências estão disponíveis."

airflow-apply: airflow-manifests
	@echo "Aplicando Airflow..."
	@kubectl apply -f .tmp/k8s/airflow/deployment.yaml
	@kubectl apply -f .tmp/k8s/airflow/scheduler.yaml
	@kubectl apply -f .tmp/k8s/airflow/dag-processor.yaml
	@kubectl apply -f .tmp/k8s/airflow/service.yaml
	@kubectl delete job airflow-db-migrate -n banvic --ignore-not-found
	@kubectl apply -f .tmp/k8s/airflow/migrate-job.yaml

terraform-apply:
	@echo "Aplicando infraestrutura Terraform..."
	@cd terraform && \
		TF_VAR_postgres_password="$(POSTGRES_PASSWORD)" \
		terraform init -input=false && \
		TF_VAR_postgres_password="$(POSTGRES_PASSWORD)" \
		terraform apply -auto-approve -input=false

env:
	@echo "POSTGRES_DB=$(POSTGRES_DB)"
	@echo "POSTGRES_USER=$(POSTGRES_USER)"
	@echo "POSTGRES_PORT=$(POSTGRES_PORT)"
	@echo "TARGET_POSTGRES_PASSWORD=***"

env-test:
	@env | grep -E '^(POSTGRES_DB|POSTGRES_USER|POSTGRES_PORT|TARGET_POSTGRES_PASSWORD)='

kind-check:
	@if kind get clusters 2>/dev/null | grep -qx "$(KIND_CLUSTER)"; then \
		echo "Kind cluster '$(KIND_CLUSTER)' já existe."; \
	else \
		echo "Criando Kind cluster '$(KIND_CLUSTER)'..."; \
		kind create cluster --name "$(KIND_CLUSTER)"; \
	fi
	@kubectl config use-context "$(KIND_CONTEXT)" >/dev/null
	@kubectl wait --for=condition=Ready node --all --timeout=120s >/dev/null
	@echo "Contexto Kubernetes: $(KIND_CONTEXT)"
	@echo "Node Kubernetes: Ready"

airflow-image:
	@echo "Construindo imagem do Airflow..."
	@docker build -t banvic-airflow:3.3.1 -f docker/airflow/Dockerfile .
	@echo "Carregando imagem no cluster Kind..."
	@kind load docker-image banvic-airflow:3.3.1 --name $(KIND_CLUSTER)
	@echo "Imagem do Airflow disponível no Kind."


airflow-manifests:
	@mkdir -p .tmp/k8s/airflow
	@for file in k8s/airflow/*.yaml; do \
		sed \
			-e "s|\$$(POSTGRES_USER)|$${POSTGRES_USER}|g" \
			-e "s|\$$(POSTGRES_DB)|$${POSTGRES_DB}|g" \
			-e "s|\$$(POSTGRES_PASSWORD)|$${POSTGRES_PASSWORD}|g" \
			-e "s|\$$(TARGET_POSTGRES_PASSWORD)|$${TARGET_POSTGRES_PASSWORD}|g" \
			-e "s|\$$(TARGET_POSTGRES_SQLALCHEMY_URL)|postgresql+psycopg://$${POSTGRES_USER}:$${TARGET_POSTGRES_PASSWORD}@banvic-postgres:5432/$${POSTGRES_DB}|g" \
			-e "s|\$$(AIRFLOW_FERNET_KEY)|$${AIRFLOW_FERNET_KEY}|g" \
			-e "s|\$$(AIRFLOW_JWT_SECRET)|$${AIRFLOW_JWT_SECRET}|g" \
			-e "s|\$$(AIRFLOW_API_SECRET_KEY)|$${AIRFLOW_API_SECRET_KEY}|g" \
			"$$file" > ".tmp/$$file"; \
	done
	@echo "Manifests Airflow renderizados em .tmp/k8s/airflow/"
	
postgres-wait:
	@echo "Aguardando PostgreSQL..."
	@kubectl wait \
		--for=condition=Available \
		deployment/banvic-postgres \
		-n banvic \
		--timeout=120s
	@echo "PostgreSQL pronto."

airflow-wait:
	@echo "Aguardando Airflow API Server..."
	@kubectl wait \
		--for=condition=Available \
		deployment/airflow \
		-n banvic \
		--timeout=120s
	@echo "Airflow API Server pronto."

scheduler-wait:
	@echo "Aguardando Airflow Scheduler..."
	@kubectl wait \
		--for=condition=Available \
		deployment/airflow-scheduler \
		-n banvic \
		--timeout=120s
	@echo "Airflow Scheduler pronto."

dag-processor-wait:
	@echo "Aguardando Airflow DAG Processor..."
	@kubectl wait \
		--for=condition=Available \
		deployment/airflow-dag-processor \
		-n banvic \
		--timeout=120s
	@echo "Airflow DAG Processor pronto."

migration-wait:
	@echo "Aguardando migração do banco Airflow..."
	@kubectl wait \
		--for=condition=complete \
		job/airflow-db-migrate \
		-n banvic \
		--timeout=120s
	@echo "Migração do banco Airflow concluída."

airflow:
	@AIRFLOW_USER=$$(kubectl exec -n banvic deployment/airflow -- \
		airflow config get-value core simple_auth_manager_users | cut -d: -f1); \
	AIRFLOW_PASSWORD=$$(kubectl exec -n banvic deployment/airflow -- \
		python -c 'import json; print(json.load(open("/opt/airflow/simple_auth_manager_passwords.json.generated"))["'$${AIRFLOW_USER}'"])'); \
	echo ""; \
	echo "======================================"; \
	echo "              AIRFLOW"; \
	echo "======================================"; \
	echo "URL:      http://localhost:8080"; \
	echo "Usuário:  $${AIRFLOW_USER}"; \
	echo "Senha:    $${AIRFLOW_PASSWORD}"; \
	echo "======================================"; \
	echo "Pressione Ctrl+C para encerrar."; \
	echo ""; \
	kubectl port-forward -n banvic deployment/airflow 8080:8080

metadata-manifest:
	@mkdir -p .tmp/k8s/postgres
	@kubectl create configmap banvic-metadata-init \
		--namespace banvic \
		--from-file=001_metadata.sql=postgres/init/001_metadata.sql \
		--dry-run=client \
		-o yaml > .tmp/k8s/postgres/metadata-init-configmap.yaml
	@echo "ConfigMap de metadata renderizado."

metadata-init: metadata-manifest postgres-wait
	@kubectl apply -f .tmp/k8s/postgres/metadata-init-configmap.yaml
	@kubectl delete job banvic-metadata-init -n banvic --ignore-not-found
	@kubectl apply -f k8s/postgres/metadata-init-job.yaml
	@kubectl wait \
		--for=condition=complete \
		job/banvic-metadata-init \
		-n banvic \
		--timeout=120s
	@echo "Metadata do pipeline inicializado."