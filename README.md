# BanVic Data Platform

## Objetivo

POC de uma plataforma de dados para ingestão das tabelas ERP fornecidas pelo BanVic.

A solução foi construída para demonstrar:

- infraestrutura local reproduzível;
- provisionamento com Terraform;
- execução em Kubernetes;
- ingestão com Meltano;
- orquestração com Apache Airflow;
- persistência em PostgreSQL;
- retries e idempotência;
- validação dos dados.

## Arquitetura

```text
CSV
 │
 ▼
Meltano
 │
 ▼
PostgreSQL
 ├── raw
 ├── metadata
 └── analytics
       ▲
       │
    Airflow
       │
       ▼
 Kubernetes
       ▲
       │
   Terraform
```

### Fluxo

1. Os arquivos CSV fornecidos pelo desafio são utilizados como fonte.
2. O Meltano utiliza `tap-csv` para extração.
3. O `target-postgres` carrega os dados na camada `raw`.
4. O Airflow orquestra a execução do Meltano.
5. PostgreSQL atua como destino da ingestão.
6. Kubernetes executa os componentes da solução.
7. Terraform provisiona a infraestrutura PostgreSQL.

## Stack

| Tecnologia | Responsabilidade |
|---|---|
| Docker | Containerização |
| Kubernetes / Kind | Execução local dos serviços |
| Terraform | Infraestrutura como código |
| Apache Airflow | Orquestração |
| Meltano | Ingestão |
| PostgreSQL | Persistência |
| Python | DAGs e configuração |
| Make | Automação da execução local |
| DBeaver | Validação dos dados |

## Estrutura do projeto

```text
banvic-data-platform/
├── airflow/
│   └── dags/
│       └── banvic_ingestion.py
├── data/
│   └── raw/
├── docker/
│   └── airflow/
│       └── Dockerfile
├── k8s/
│   └── airflow/
│       ├── dag-processor.yaml
│       ├── deployment.yaml
│       ├── migrate-job.yaml
│       ├── scheduler.yaml
│       └── service.yaml
├── meltano/
│   └── csv_files_definition.json
├── postgres/
│   └── deployment.yaml
├── terraform/
│   ├── deployment.tf
│   ├── pvc.tf
│   └── service.tf
├── .env
├── Makefile
├── meltano.yml
└── README.md
```

## Execução local

### Pré-requisitos

- Docker
- Kind
- kubectl
- Terraform
- Make

### Subir o ambiente

```bash
make run
```

O comando:

1. verifica ou cria o cluster Kind;
2. aplica a infraestrutura PostgreSQL via Terraform;
3. aguarda o PostgreSQL;
4. renderiza e aplica os manifests do Airflow;
5. executa a migration do banco do Airflow;
6. aguarda API Server, Scheduler e DAG Processor.

### Acessar o Airflow

```bash
kubectl port-forward -n banvic deployment/airflow 8080:8080
```

Acessar:

```text
http://localhost:8080
```

### Executar a pipeline

Na interface do Airflow:

```text
banvic_ingestion
        │
        ▼
ingestao_meltano
        │
        ▼
Meltano
        │
        ▼
PostgreSQL
```

### Validar os dados

A validação dos dados pode ser realizada no PostgreSQL utilizando o DBeaver.

## Pipeline

A DAG `banvic_ingestion` executa o Meltano utilizando:

```text
tap-csv → target-postgres
```

As sete tabelas fornecidas pelo desafio são carregadas na camada `raw`:

```text
raw.agencias
raw.clientes
raw.colaborador_agencia
raw.colaboradores
raw.conta
raw.proposta_credito
raw.transacoes
```

### Resiliência

A DAG possui:

```python
retries = 2
retry_delay = 1 minuto
```

O comportamento de retry foi validado através de uma falha controlada da task.

### Idempotência

A pipeline foi executada consecutivamente mais de uma vez.

Os quantitativos permaneceram estáveis, sem duplicação dos registros.

```text
agencias              10
clientes             998
colaborador_agencia  100
colaboradores        100
conta                999
proposta_credito    2000
transacoes         71999
```

Total:

```text
76.106 registros
```

### Qualidade dos dados

Foram realizadas validações de:

- duplicidade das chaves;
- valores `NULL` nas chaves;
- integridade referencial;
- datas inválidas;
- valores financeiros.

Os registros órfãos encontrados na fonte são preservados na camada `raw`, mantendo a representação original dos dados recebidos.

## Status

- [x] Docker
- [x] Kubernetes / Kind
- [x] Terraform
- [x] PostgreSQL
- [x] Meltano
- [x] Airflow
- [x] DAG de ingestão
- [x] Ingestão das 7 tabelas
- [x] Retry
- [x] Idempotência
- [x] Validação dos dados
- [x] Automação com Makefile
- [x] Documentação inicial
