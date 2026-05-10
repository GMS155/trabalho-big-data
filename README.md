# PySpark Vehicle Telemetry API

API REST construída com **FastAPI** + **PySpark**, backed por um cluster **HDFS** (Hadoop) e **Spark Standalone**. Um job de Structured Streaming monitora arquivos CSV enviados ao HDFS e os processa em Parquet. Os endpoints de análise lêem os dados processados via Spark batch.

## Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) e [Docker Compose](https://docs.docker.com/compose/) instalados
- Porta `8000`, `8080`, `8081`, `9000` e `9870` livres na máquina

## Estrutura do projeto

```
pyspark-api-project/
├── data/
│   └── vehicle_data.csv        # Dataset carregado automaticamente no HDFS
├── hadoop/
│   ├── config/                 # core-site.xml, hdfs-site.xml
│   └── init/init-hdfs.sh       # Script de inicialização do HDFS
├── scripts/
│   └── ingest.sh               # Envia novo CSV ao HDFS para o streaming processar
├── spark/conf/
│   └── spark-defaults.conf
├── src/
│   ├── app.py                  # Entry-point FastAPI
│   ├── api/routes.py           # Endpoints REST
│   ├── schemas/vehicle.py      # Schema do dataset
│   └── spark/
│       ├── jobs.py             # Funções de batch (Spark)
│       └── streaming.py        # Job de Structured Streaming
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Como executar

### 1. Subir o cluster completo

```bash
docker compose up -d --build
```

Isso inicializa na ordem correta:

| Container | Função |
|---|---|
| `namenode` | HDFS NameNode |
| `datanode` | HDFS DataNode |
| `hdfs-init` | Cria pastas no HDFS e faz upload do `vehicle_data.csv` |
| `spark-master` | Spark Master |
| `spark-worker` | Spark Worker (2 cores, 2 GB) |
| `api` | FastAPI + Spark Driver + Streaming job |

> O container `hdfs-init` encerra com código 0 ao concluir — isso é **esperado**.

### 2. Aguardar a API ficar pronta

A API demora ~60–90 s para criar o SparkSession. Verifique o health:

```bash
curl http://localhost:8000/health
# {"status":"healthy"}  ← pronto
# {"status":"starting"} ← aguarde mais um pouco
```

Ou acompanhe os logs:

```bash
docker logs -f api
```

### 3. Ingerir novos dados (opcional)

Para enviar um novo arquivo CSV ao HDFS e acionar o pipeline de streaming:

```bash
./scripts/ingest.sh data/vehicle_data.csv
```

O arquivo será processado em até 30 segundos e gravado em Parquet em `/processed/`.

## Endpoints disponíveis

A documentação interativa está em **http://localhost:8000/docs**.

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `GET` | `/health` | Status da API e do SparkSession |
| `GET` | `/data` | Registros paginados (`limit`, `offset`) |
| `GET` | `/vehicles` | Lista de IDs de veículos |
| `GET` | `/vehicles/{veh_id}/trips` | Viagens de um veículo |
| `GET` | `/trips/{trip_id}` | Telemetria de uma viagem (`limit`) |
| `GET` | `/stats/speed` | Estatísticas globais de velocidade (avg/min/max) |
| `GET` | `/analytics/speeding` | Eventos de excesso de velocidade (`threshold`) |
| `GET` | `/analytics/routes` | Rotas mais utilizadas via grid lat/lon |
| `GET` | `/analytics/stops` | Paradas longas (velocidade = 0) |
| `GET` | `/analytics/fuel` | Consumo estimado de combustível via MAF |
| `GET` | `/analytics/rpm-ranking` | Ranking de eficiência por RPM médio |
| `GET` | `/anomalies` | Anomalias detectadas via z-score em RPM/MAF |

## Interfaces web

| Interface | URL |
|---|---|
| API Docs (Swagger) | http://localhost:8000/docs |
| HDFS Web UI | http://localhost:9870 |
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |

## Parar o ambiente

```bash
# Parar mantendo os volumes (dados HDFS preservados)
docker compose down

# Parar e remover todos os dados
docker compose down -v
```

## Dependencies

This project requires the following Python packages:

- Flask
- PySpark

These dependencies are listed in the `requirements.txt` file and will be installed automatically when building the Docker image.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.