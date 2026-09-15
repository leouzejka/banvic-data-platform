ifneq (,$(wildcard .env))
include .env
export
endif

KIND_CLUSTER := banvic
KIND_CONTEXT := kind-$(KIND_CLUSTER)

.PHONY: run env env-test kind-check terraform-apply airflow-manifests airflow-apply postgres-wait airflow-wait scheduler-wait dag-processor-wait migration-wait

run: kind-check terraform-apply postgres-wait airflow-apply migration-wait airflow-wait scheduler-wait dag-processor-wait
	@echo "Cluster Kubernetes e infraestrutura prontos."

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