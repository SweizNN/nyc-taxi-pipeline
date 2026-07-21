#!/usr/bin/env bash
# =============================================================================
# NYC Taxi Pipeline — Kubernetes Local Çalıştırma Scripti
# =============================================================================
# Gereksinimler: kubectl, minikube VEYA kind VEYA Docker Desktop K8s
#
# Kullanım:
#   chmod +x scripts/k8s-local-run.sh
#   ./scripts/k8s-local-run.sh
#
# Adımları tek tek çalıştırmak için:
#   ./scripts/k8s-local-run.sh infra      → Sadece altyapı
#   ./scripts/k8s-local-run.sh pipeline   → Pipeline job'larını başlat
#   ./scripts/k8s-local-run.sh ui         → Web UI'ya port-forward
#   ./scripts/k8s-local-run.sh status     → Pod durumlarını göster
#   ./scripts/k8s-local-run.sh clean      → Her şeyi sil
# =============================================================================

set -euo pipefail

NS="nyc-taxi-pipeline"
MANIFESTS="ml_pipeline"
REGISTRY="${IMAGE_REGISTRY:-ghcr.io/sweiznn}"  # Kendi registry'ni .env veya ortam değişkeni ile ayarla

# ── Renkli çıktı ─────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; RED='\033[0;31m'; NC='\033[0m'
info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── kubectl kontrolü ──────────────────────────────────────
check_kubectl() {
    command -v kubectl >/dev/null 2>&1 || error "kubectl bulunamadı. Kurun: https://kubernetes.io/docs/tasks/tools/"
    kubectl cluster-info >/dev/null 2>&1 || error "Kubernetes cluster'a bağlanılamıyor. Minikube/Kind/Docker Desktop çalışıyor mu?"
    info "Kubernetes cluster'a bağlanıldı: $(kubectl config current-context)"
}

# ── Manifest'lerdeki <your-registry> yerini doldur ────────
inject_registry() {
    info "Registry adresi manifest'lere ekleniyor: ${REGISTRY}"
    find "${MANIFESTS}/" -name "*.yml" | while read -r f; do
        sed -i.bak "s|<your-registry>|${REGISTRY}|g" "$f"
    done
}

# ── 1. ALTYAPI ────────────────────────────────────────────
deploy_infra() {
    info "=== AŞAMA 1: Altyapı Kurulumu ==="

    info "Namespace oluşturuluyor..."
    kubectl apply -f ${MANIFESTS}/00-namespace.yml
    success "Namespace: ${NS}"

    info "PersistentVolumeClaims oluşturuluyor..."
    kubectl apply -f ${MANIFESTS}/01-pvc.yml
    success "PVC'ler: mlflow-pvc (5Gi), spark-data-pvc (20Gi)"

    info "Spark RBAC (ServiceAccount + Role) oluşturuluyor..."
    kubectl apply -f ${MANIFESTS}/05-spark-rbac.yml
    success "RBAC hazır"

    info "Kafka + Zookeeper başlatılıyor..."
    kubectl apply -f ${MANIFESTS}/02-kafka.yml

    info "Kafka pod'unun hazır olması bekleniyor (max 5 dakika)..."
    kubectl rollout status statefulset/kafka -n ${NS} --timeout=300s
    success "Kafka hazır!"

    info "MLflow başlatılıyor..."
    kubectl apply -f ${MANIFESTS}/03-mlflow.yml
    kubectl rollout status deployment/mlflow -n ${NS} --timeout=120s
    success "MLflow hazır!"

    info "Dashboard başlatılıyor..."
    kubectl apply -f ${MANIFESTS}/04-dashboard.yml
    success "Dashboard pod'u başlatıldı"

    echo ""
    success "=== Altyapı Kurulumu Tamamlandı ==="
    kubectl get pods -n ${NS}
}

# ── 2. STREAM-TO-BRONZE (sürekli çalışan deployment) ──────
deploy_streaming() {
    info "=== AŞAMA 2: Stream-to-Bronze Başlatılıyor ==="
    kubectl apply -f ${MANIFESTS}/11-deployment-stream-to-bronze.yml
    info "Stream-to-Bronze deployment başlatıldı. Kafka'dan veri bekleniyor..."
    kubectl get pods -n ${NS} -l app=stream-to-bronze
}

# ── 3. PRODUCER (Kafka'ya veri gönder) ───────────────────
run_producer() {
    info "=== AŞAMA 3: Producer Job Başlatılıyor ==="
    warn "ÖNEMLİ: Parquet dosyasının PVC'de olduğundan emin olun!"
    warn "Kopyalamak için: kubectl cp data/raw/yellow_tripdata_2023-01.parquet \\"
    warn "  \$(kubectl get pod -n ${NS} -l app=stream-to-bronze -o jsonpath='{.items[0].metadata.name}'):/data/raw/ -n ${NS}"
    echo ""

    # Eski job varsa sil (yeniden çalıştırma için)
    kubectl delete job producer-job -n ${NS} --ignore-not-found=true

    kubectl apply -f ${MANIFESTS}/06-job-producer.yml
    info "Producer job başlatıldı. Log'ları izle:"
    info "  kubectl logs -f job/producer-job -n ${NS}"
    kubectl get pods -n ${NS} -l app=producer
}

# ── 4. BATCH JOB'LAR (sırayla) ───────────────────────────
run_batch_jobs() {
    info "=== AŞAMA 4: Batch Job'lar (Bronze→Silver→Gold→Training) ==="

    # Eski job'ları temizle
    for job in bronze-to-silver-job silver-to-gold-job model-training-job; do
        kubectl delete job $job -n ${NS} --ignore-not-found=true
    done

    # Bronze → Silver
    info "Bronze→Silver job başlatılıyor..."
    kubectl apply -f ${MANIFESTS}/12-job-bronze-to-silver.yml
    info "Tamamlanması bekleniyor..."
    kubectl wait --for=condition=complete job/bronze-to-silver-job -n ${NS} --timeout=600s
    success "Bronze→Silver tamamlandı!"

    # Silver → Gold
    info "Silver→Gold job başlatılıyor..."
    kubectl apply -f ${MANIFESTS}/13-job-silver-to-gold.yml
    kubectl wait --for=condition=complete job/silver-to-gold-job -n ${NS} --timeout=600s
    success "Silver→Gold tamamlandı!"

    # Model Training
    info "Model Training job başlatılıyor..."
    kubectl apply -f ${MANIFESTS}/10-job-model-training.yml
    info "Eğitim sürüyor (bu birkaç dakika sürebilir)..."
    kubectl wait --for=condition=complete job/model-training-job -n ${NS} --timeout=900s
    success "Model eğitimi tamamlandı! MLflow'da sonuçları gör."
}

# ── 5. WEB UI (port-forward) ─────────────────────────────
open_ui() {
    info "=== WEB ARAYÜZÜ ==="
    echo ""
    echo "  MLflow UI  → http://localhost:5000"
    echo "  Dashboard  → http://localhost:8501"
    echo ""
    warn "CTRL+C ile durdurmak için. İki terminal gerekiyor:"
    echo ""
    echo "  Terminal 1: kubectl port-forward svc/mlflow-service 5000:5000 -n ${NS}"
    echo "  Terminal 2: kubectl port-forward svc/dashboard-service 8501:80 -n ${NS}"
    echo ""

    # Arka planda port-forward başlat
    kubectl port-forward svc/mlflow-service 5000:5000 -n ${NS} &
    PF1=$!
    kubectl port-forward svc/dashboard-service 8501:80 -n ${NS} &
    PF2=$!

    success "Port-forward'lar başlatıldı!"
    echo "  Tarayıcıda: http://localhost:5000 (MLflow) | http://localhost:8501 (Dashboard)"
    echo ""
    echo "Durdurmak için ENTER'a basın..."
    read -r
    kill $PF1 $PF2 2>/dev/null || true
}

# ── DURUM ────────────────────────────────────────────────
show_status() {
    info "=== POD DURUMU (Namespace: ${NS}) ==="
    kubectl get pods -n ${NS} -o wide
    echo ""
    info "=== SERVİSLER ==="
    kubectl get svc -n ${NS}
    echo ""
    info "=== JOB'LAR ==="
    kubectl get jobs -n ${NS}
    echo ""
    info "=== PVC'LER ==="
    kubectl get pvc -n ${NS}
}

# ── TEMİZLE ─────────────────────────────────────────────
clean_all() {
    warn "Namespace '${NS}' ve tüm kaynaklar silinecek!"
    read -rp "Devam etmek istiyor musun? (evet/hayir): " confirm
    if [[ "$confirm" == "evet" ]]; then
        kubectl delete namespace ${NS} --ignore-not-found=true
        success "Namespace silindi. Her şey temizlendi."
    else
        info "İptal edildi."
    fi
}

# ── TAM PIPELINE (hepsini sırayla çalıştır) ───────────────
run_full() {
    check_kubectl
    inject_registry
    deploy_infra
    deploy_streaming
    run_producer
    info "Producer'ın bitmesini bekliyoruz (max 5 dakika)..."
    kubectl wait --for=condition=complete job/producer-job -n ${NS} --timeout=300s
    run_batch_jobs
    show_status
    open_ui
}

# ── ANA MENÜ ─────────────────────────────────────────────
case "${1:-full}" in
    infra)    check_kubectl && inject_registry && deploy_infra ;;
    stream)   deploy_streaming ;;
    producer) run_producer ;;
    pipeline) run_batch_jobs ;;
    ui)       open_ui ;;
    status)   show_status ;;
    clean)    clean_all ;;
    full)     run_full ;;
    *)
        echo "Kullanım: $0 [infra|stream|producer|pipeline|ui|status|clean|full]"
        echo ""
        echo "  full      → Hepsini sırayla çalıştır (varsayılan)"
        echo "  infra     → Namespace, PVC, Kafka, MLflow, Dashboard"
        echo "  stream    → Stream-to-Bronze Deployment başlat"
        echo "  producer  → Producer Job başlat (Kafka'ya veri gönder)"
        echo "  pipeline  → Batch job'ları sırayla çalıştır (B→S→G→Training)"
        echo "  ui        → MLflow ve Dashboard'a port-forward"
        echo "  status    → Pod/Service/Job durumlarını göster"
        echo "  clean     → Tüm kaynakları sil"
        exit 1
        ;;
esac
