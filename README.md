# NYC Yellow Taxi — Kubernetes Big Data Pipeline

End-to-end big data pipeline. Her işlem gerçek bir Kubernetes pod'unda çalışır:
Kafka Producer → Spark Structured Streaming → Delta Lake (Bronze/Silver/Gold) → ML Training → Dashboard

```
                 GitHub Actions CI/CD
                        │
              ┌──────── push ────────┐
              │                      │
         Build Images           kubectl apply
              │                      │
     ─────────────────────────────────────────────────────
              │   Kubernetes Cluster (nyc-taxi-pipeline)  │
              │                                           │
     [StatefulSet] Zookeeper   [StatefulSet] Kafka        │
              │                      │                    │
     [Job] Producer ──────────────── ▼                    │
              │              Kafka Topic                  │
              │                      │                    │
     [Deployment] stream-to-bronze ──▼                    │
              │              Delta Bronze                 │
              │                      │                    │
     [Job] bronze-to-silver ─────────▼                    │
              │              Delta Silver                 │
              │                      │                    │
     [Job] silver-to-gold ───────────▼                    │
              │              Delta Gold                   │
              │                      │                    │
     [Job] model-training ───────────▼                    │
              │              MLflow Experiments           │
              │                                           │
     [Deployment] MLflow    [Deployment] Dashboard        │
     ─────────────────────────────────────────────────────
```

## Kubernetes Manifest Dosyaları (`ml_pipeline/`)

| Dosya | Tür | Açıklama |
|-------|-----|----------|
| `00-namespace.yml` | Namespace | `nyc-taxi-pipeline` namespace |
| `01-pvc.yml` | PVC | `mlflow-pvc` (5Gi) + `spark-data-pvc` (20Gi) |
| `02-kafka.yml` | StatefulSet + Service | Kafka + Zookeeper |
| `03-mlflow.yml` | Deployment + Service | MLflow tracking + NodePort 30500 |
| `04-dashboard.yml` | Deployment + Service | Streamlit dashboard + NodePort 30880 |
| `05-spark-rbac.yml` | RBAC | Spark için ServiceAccount + Role |
| `06-job-producer.yml` | Job | Parquet → Kafka producer |
| `10-job-model-training.yml` | Job | Spark ML training |
| `11-deployment-stream-to-bronze.yml` | Deployment | Kafka → Delta Bronze streaming |
| `12-job-bronze-to-silver.yml` | Job | Delta Bronze → Silver batch |
| `13-job-silver-to-gold.yml` | Job | Delta Silver → Gold batch |

## Veri Dosyaları

```
data/
├── raw/
│   └── yellow_tripdata_2023-01.parquet   # TLC'den indir
└── lookup/
    └── taxi_zone_lookup.csv              # TLC'den indir
```

İndirme linkleri:
- https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
- https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

## 1. CI/CD — GitHub Actions Kurulumu

### GitHub Secrets Ekleme

`Settings > Secrets and variables > Actions > New repository secret`:

| Secret | Değer | Ne için? |
|--------|-------|---------|
| `KUBECONFIG` | `~/.kube/config` içeriği | K8s cluster erişimi |

> **Not:** GHCR için ayrıca Docker Hub secret'ı gerekmez. `GITHUB_TOKEN` otomatik kullanılır.

### Trigger

```bash
git push origin main
# Actions sekmesinde: Build & Push → Deploy to Kubernetes
```

## 2. Local Terminal — Adım Adım Öğrenim

### Gereksinimler

- `kubectl` kurulu
- Minikube, Kind veya Docker Desktop Kubernetes aktif

### Otomatik Script

```powershell
# WSL veya Git Bash'te:
chmod +x scripts/k8s-local-run.sh

# Tüm pipeline'ı sırayla çalıştır:
./scripts/k8s-local-run.sh full

# Veya adım adım:
./scripts/k8s-local-run.sh infra      # Kafka, MLflow, Dashboard
./scripts/k8s-local-run.sh stream     # Stream-to-Bronze başlat
./scripts/k8s-local-run.sh producer   # Kafka'ya veri gönder
./scripts/k8s-local-run.sh pipeline   # Bronze→Silver→Gold→Training
./scripts/k8s-local-run.sh ui         # Web UI port-forward
./scripts/k8s-local-run.sh status     # Pod durumları
./scripts/k8s-local-run.sh clean      # Her şeyi sil
```

### Manuel Adımlar

```powershell
# ── ALTYAPI ────────────────────────────────────────────────
kubectl apply -f ml_pipeline/00-namespace.yml
kubectl apply -f ml_pipeline/01-pvc.yml
kubectl apply -f ml_pipeline/05-spark-rbac.yml
kubectl apply -f ml_pipeline/02-kafka.yml

# Kafka'yı izle:
kubectl get pods -n nyc-taxi-pipeline -w

# ── VERİYİ PVC'YE KOPYALA ─────────────────────────────────
# Önce herhangi bir çalışan pod'u bul:
$POD = kubectl get pod -n nyc-taxi-pipeline -o jsonpath='{.items[0].metadata.name}'
kubectl cp data/raw/yellow_tripdata_2023-01.parquet ${POD}:/data/raw/ -n nyc-taxi-pipeline
kubectl cp data/lookup/taxi_zone_lookup.csv ${POD}:/data/lookup/ -n nyc-taxi-pipeline

# ── MLFLow + DASHBOARD ────────────────────────────────────
kubectl apply -f ml_pipeline/03-mlflow.yml
kubectl apply -f ml_pipeline/04-dashboard.yml

# ── STREAM-TO-BRONZE (sürekli çalışır) ─────────────────────
kubectl apply -f ml_pipeline/11-deployment-stream-to-bronze.yml

# Streaming log'larını izle:
kubectl logs -f deployment/stream-to-bronze -n nyc-taxi-pipeline

# ── PRODUCER (Kafka'ya veri gönder) ─────────────────────────
kubectl apply -f ml_pipeline/06-job-producer.yml
kubectl logs -f job/producer-job -n nyc-taxi-pipeline

# ── BATCH JOB'LAR (sırayla) ─────────────────────────────────
kubectl apply -f ml_pipeline/12-job-bronze-to-silver.yml
kubectl wait --for=condition=complete job/bronze-to-silver-job -n nyc-taxi-pipeline --timeout=600s

kubectl apply -f ml_pipeline/13-job-silver-to-gold.yml
kubectl wait --for=condition=complete job/silver-to-gold-job -n nyc-taxi-pipeline --timeout=600s

kubectl apply -f ml_pipeline/10-job-model-training.yml
kubectl wait --for=condition=complete job/model-training-job -n nyc-taxi-pipeline --timeout=900s

# ── DURUM KONTROLÜ ─────────────────────────────────────────
kubectl get pods -n nyc-taxi-pipeline
kubectl get jobs -n nyc-taxi-pipeline
kubectl get pvc  -n nyc-taxi-pipeline
```

## 3. Web Arayüzleri

### Port-Forward ile Erişim

```powershell
# Terminal 1 — MLflow (Deney takibi, model karşılaştırma):
kubectl port-forward svc/mlflow-service 5000:5000 -n nyc-taxi-pipeline
# → http://localhost:5000

# Terminal 2 — Dashboard (EDA, model metrikleri, görselleştirmeler):
kubectl port-forward svc/dashboard-service 8501:80 -n nyc-taxi-pipeline
# → http://localhost:8501
```

### NodePort ile Erişim (Minikube)

```powershell
minikube service mlflow-nodeport -n nyc-taxi-pipeline --url     # port 30500
minikube service dashboard-nodeport -n nyc-taxi-pipeline --url  # port 30880
```

## Faydalı kubectl Komutları

```powershell
# Pod log'larını canlı izle:
kubectl logs -f <pod-adi> -n nyc-taxi-pipeline

# Pod içine bağlan:
kubectl exec -it <pod-adi> -n nyc-taxi-pipeline -- bash

# Job'u yeniden çalıştır:
kubectl delete job bronze-to-silver-job -n nyc-taxi-pipeline
kubectl apply -f ml_pipeline/12-job-bronze-to-silver.yml

# Silver içeriğini sorgula (spark pod içinde):
kubectl exec -it deployment/stream-to-bronze -n nyc-taxi-pipeline -- \
  spark-sql -e "SELECT pickup_borough, COUNT(*) FROM delta.\`/data/delta/silver/yellow_taxi_trips\` GROUP BY 1"

# Tüm namespace'i sil (temizlik):
kubectl delete namespace nyc-taxi-pipeline
```

## Ortam Değişkenleri

| Değişken | Varsayılan | Açıklama |
|----------|-----------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka.nyc-taxi-pipeline.svc.cluster.local:9092` | Kafka adresi |
| `DELTA_BRONZE_PATH` | `/data/delta/bronze/yellow_taxi_trips` | Bronze Delta yolu |
| `DELTA_SILVER_PATH` | `/data/delta/silver/yellow_taxi_trips` | Silver Delta yolu |
| `DELTA_GOLD_PATH` | `/data/delta/gold/fare_features` | Gold Delta yolu |
| `MLFLOW_TRACKING_URI` | `http://mlflow-service...:5000` | MLflow sunucu |
| `PRODUCER_MAX_ROWS` | `10000` | Kafka'ya gönderilecek satır sayısı |
| `PRODUCER_DRY_RUN` | `false` | `true` → Kafka'ya bağlanmadan test |

## Mimari Kararlar

- **`--deploy-mode client`**: Spark Job pod'u driver olarak çalışır; K8s executor pod'larını otomatik başlatır. `cluster` modu ikinci bir driver pod oluşturup çakışma yaratırdı.
- **`spark-data-pvc`**: Tüm Spark pod'ları aynı Delta Lake verilerine erişmek için bu PVC'yi paylaşır. Üretimde MinIO/S3 önerilir.
- **Dashboard PySpark-free**: Dashboard pod'u artık sadece Pandas ile Parquet okur; ~2GB → ~500MB imaj boyutu.
- **GHCR**: GitHub Container Registry, `GITHUB_TOKEN` ile secret gerekmeden çalışır.
