# BanVic Data Platform — arquitetura e roteiro técnico

Este documento descreve a organização atual do projeto BanVic e serve como
base para a explicação da solução em uma apresentação ou vídeo.

## 1. Arquitetura

```text
Arquivos CSV
    │
    ▼
Airflow persistente no Kind
    │ KubernetesPodOperator
    ▼
Pod Meltano efêmero
    │ tap-csv → target-postgres
    ▼ TCP
PostgreSQL externo e persistente
    ├── airflow
    └── banvic_database
        ├── raw
        ├── metadata
        └── analytics
```

Responsabilidades:

- PostgreSQL: armazenamento persistente.
- Airflow: orquestração permanente.
- DAG: dependências e disparo do ELT.
- Meltano: extração e carga.
- Kubernetes/Kind: execução dos Pods Meltano.
- Terraform: provisionamento do namespace Kubernetes.
- Docker: empacotamento das aplicações.

O PostgreSQL não é executado no Kind. O Meltano não é um serviço permanente.
Cada execução do Meltano ocorre em um Pod criado pelo Airflow e removido ao
final da execução.

O projeto não utiliza dbt.

## 2. Organização das pastas

```text
banvic-data-platform/
├── airflow/
│   └── dags/banvic_ingestion.py
├── data/raw/
│   ├── agencias.csv
│   ├── clientes.csv
│   ├── colaborador_agencia.csv
│   ├── colaboradores.csv
│   ├── contas.csv
│   ├── propostas_credito.csv
│   └── transacoes.csv
├── docker/
│   ├── airflow/Dockerfile
│   └── meltano/Dockerfile
├── k8s/
│   ├── namespace.yaml
│   └── airflow/
│       ├── deployment.yaml
│       ├── scheduler.yaml
│       ├── dag-processor.yaml
│       ├── migrate-job.yaml
│       ├── postgres-secret.yaml
│       ├── rbac.yaml
│       └── service.yaml
├── meltano/
│   └── csv_files_definition.json
├── plugins/
│   ├── extractors/
│   └── loaders/
├── postgres/init/
│   ├── init.sql
│   └── 001_metadata.sql
├── terraform/
│   ├── main.tf
│   └── namespace.tf
├── docker-compose.yml
├── meltano.yml
├── Makefile
├── README.md
└── .env.example
```

### Airflow

`airflow/dags/banvic_ingestion.py` define três tasks:

```text
start_execution → ingestao_meltano → finalize_execution
```

- `start_execution` registra a execução como `RUNNING`.
- `ingestao_meltano` usa `KubernetesPodOperator`.
- `finalize_execution` grava `SUCCESS` ou `FAILED` e calcula
  `records_processed`.

A DAG usa retries padrão de duas tentativas com intervalo de um minuto.
A task final usa `trigger_rule=ALL_DONE` e não é repetida.

O Pod Meltano usa:

- imagem `banvic-meltano:4.2.2`;
- ambiente Meltano `k8s`;
- comando `meltano --environment=k8s run tap-csv target-postgres`;
- Secret `banvic-postgres-connection`;
- `get_logs=True`;
- `on_finish_action="delete_pod"`;
- `do_xcom_push=False`.

### Dados

`data/raw` contém os sete CSVs carregados pelo `tap-csv`. O arquivo
`meltano/csv_files_definition.json` associa cada arquivo às suas entidades e
chaves.

### Docker

`docker/airflow/Dockerfile` cria a imagem do Airflow 3.3.1, instala Meltano,
os providers PostgreSQL/Kubernetes e copia a DAG.

`docker/meltano/Dockerfile` cria a imagem do Meltano 4.2.2, instala os plugins,
copia os arquivos de configuração e os dados, e executa como usuário não root.

### Kubernetes

- `deployment.yaml`: Airflow API Server.
- `scheduler.yaml`: Scheduler e variáveis do banco externo.
- `dag-processor.yaml`: DAG Processor.
- `service.yaml`: acesso interno ao API Server.
- `migrate-job.yaml`: migration do banco de metadata do Airflow.
- `postgres-secret.yaml`: conexão do Meltano com o PostgreSQL externo.
- `rbac.yaml`: ServiceAccount, Role e RoleBinding do Scheduler.

O Role `airflow-meltano-runner` permite ao Scheduler criar, consultar,
acompanhar, atualizar e remover Pods, ler logs e listar eventos no namespace
`banvic`.

### Meltano

`meltano.yml` declara o extractor `tap-csv` e o loader `target-postgres`.
No ambiente `k8s`, o loader usa `TARGET_POSTGRES_SQLALCHEMY_URL` e grava no
schema `raw`.

### PostgreSQL

`docker-compose.yml` executa o PostgreSQL 16 Alpine com o volume persistente
`banvic-postgres-data`, porta publicada e healthcheck.

Os scripts em `postgres/init` criam o banco `airflow`, os schemas `raw`,
`metadata` e `analytics`, e a tabela `metadata.pipeline_execution`.

### Terraform

O Terraform usa o provider Kubernetes e cria o namespace `banvic`. Os
workloads Airflow e seu RBAC são manifests aplicados pelo Makefile; o
Terraform não controla o processo ELT nem o PostgreSQL.

## 3. Bootstrap

Após clonar o projeto:

```bash
cp .env.example .env
```

Além das variáveis do PostgreSQL, o `.env` precisa definir:

```text
TARGET_POSTGRES_PASSWORD
AIRFLOW_FERNET_KEY
AIRFLOW_JWT_SECRET
AIRFLOW_API_SECRET_KEY
```

No Kind local, `POSTGRES_HOST` é detectado pelo Makefile usando o gateway da
rede Docker `kind`. Em outro ambiente, deve ser definido manualmente com um
endereço TCP acessível pelos Pods.

O bootstrap completo é:

```bash
make run
```

O target executa, nesta ordem:

```text
deps → postgres → kind-check → airflow-image → meltano-image
→ terraform-apply → airflow-apply → migration-wait
→ airflow-wait → scheduler-wait → dag-processor-wait
```

`deps` verifica Docker, Docker Compose, Kind, kubectl, Terraform e Make.
`postgres` inicia o banco externo antes da preparação do Kind.

## 4. Execução e lifecycle

O disparo pelo terminal é feito com:

```bash
make dag-run
```

O target executa:

```bash
kubectl exec -n banvic deployment/airflow -- \
  airflow dags trigger -o json banvic_ingestion
```

O Scheduler cria um Pod com nome semelhante a `meltano-ingestion-<sufixo>`.
O Pod executa o ELT, envia logs para o Airflow e é removido automaticamente
quando termina.

## 5. Persistência e monitoramento

Os bancos têm responsabilidades separadas:

| Banco | Responsabilidade |
|---|---|
| `airflow` | metadata database do Airflow |
| `banvic_database` | dados do desafio |

As sete tabelas raw consideradas na finalização são:

```text
raw.agencias
raw.clientes
raw.colaborador_agencia
raw.colaboradores
raw.conta
raw.proposta_credito
raw.transacoes
```

`metadata.pipeline_execution` registra `RUNNING`, `SUCCESS` ou `FAILED`,
`records_processed` e `error_message`. O registro inicial usa conflito por
`pipeline_name` e `execution_id`, mantendo a atualização idempotente da
execução.

## 6. Acesso e validação

Para acessar o Airflow:

```bash
make airflow
```

Ou:

```bash
kubectl port-forward -n banvic deployment/airflow 8080:8080
```

URL: `http://localhost:8080`

Para consultar o PostgreSQL:

```bash
docker compose exec -T postgres \
  psql -U banvic -d banvic_database
```

Contagens raw:

```sql
SELECT 'raw.agencias', COUNT(*) FROM raw.agencias
UNION ALL SELECT 'raw.clientes', COUNT(*) FROM raw.clientes
UNION ALL SELECT 'raw.colaborador_agencia', COUNT(*) FROM raw.colaborador_agencia
UNION ALL SELECT 'raw.colaboradores', COUNT(*) FROM raw.colaboradores
UNION ALL SELECT 'raw.conta', COUNT(*) FROM raw.conta
UNION ALL SELECT 'raw.proposta_credito', COUNT(*) FROM raw.proposta_credito
UNION ALL SELECT 'raw.transacoes', COUNT(*) FROM raw.transacoes;
```

Execuções:

```sql
SELECT pipeline_name, execution_id, status,
       records_processed, error_message
FROM metadata.pipeline_execution
ORDER BY id DESC;
```

## 7. Roteiro sugerido para o vídeo

1. Mostrar a arquitetura e destacar o PostgreSQL fora do Kind.
2. Mostrar a estrutura do repositório e os Dockerfiles.
3. Mostrar o volume persistente do PostgreSQL no Compose.
4. Executar ou explicar `make run`.
5. Mostrar namespace, Deployments e Pods Airflow prontos.
6. Mostrar o RBAC do Scheduler.
7. Executar `make dag-run`.
8. Mostrar o Pod Meltano e seus logs na task do Airflow.
9. Mostrar a remoção automática do Pod.
10. Consultar as sete tabelas raw.
11. Consultar `metadata.pipeline_execution`.
12. Explicar retries, idempotência e a separação dos bancos.

## 8. Limites atuais

- A fonte versionada está representada pelos CSVs; o arquivo original
  `banvic_data.zip` não está no repositório.
- O schema `analytics` existe, mas não há transformação implementada nele.
- O Makefile inicia o PostgreSQL em background e não aguarda explicitamente o
  healthcheck antes da migration do Airflow.
- As validações de dados são feitas por SQL ou ferramentas externas, sem um
  script dedicado.
