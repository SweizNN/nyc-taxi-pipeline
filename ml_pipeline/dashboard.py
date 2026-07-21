import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import mlflow
import os
import glob

# ─────────────────────────────────────────────────────────
# Sayfa konfigürasyonu
# ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NYC Taxi Fiyat Tahmini",
    page_icon="🚖",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🚖 NYC Yellow Taxi — Büyük Veri & ML Dashboard")
st.markdown(
    "Bu dashboard; Kafka, Spark ve Delta Lake üzerinden akan verilerin analizini "
    "ve MLflow ile takip edilen model sonuçlarını sunar."
)

# ─────────────────────────────────────────────────────────
# VERİ YÜKLEME
# PySpark bağımlılığı kaldırıldı. Gold Delta tablosu Parquet
# dosyaları olarak /data/delta/gold/fare_features/ altında okunur.
# Bu sayede dashboard pod'u çok daha hafif başlar.
# ─────────────────────────────────────────────────────────
GOLD_PATH = os.getenv("DELTA_GOLD_PATH", "/data/delta/gold/fare_features")


@st.cache_data(ttl=300)  # 5 dakikada bir yenile
def load_data():
    """Gold Delta katmanını Pandas ile yükle (Parquet dosyaları)."""
    parquet_files = glob.glob(f"{GOLD_PATH}/**/*.parquet", recursive=True)
    if parquet_files:
        try:
            df = pd.concat(
                [pd.read_parquet(f) for f in parquet_files],
                ignore_index=True,
            )
            st.sidebar.success(f"✅ {len(df):,} kayıt yüklendi (Gold katman)")
            return df
        except Exception as e:
            st.sidebar.warning(f"Parquet okunamadı: {e} — test verisi kullanılıyor.")

    # Gerçek veri yoksa demo verisi üret
    st.sidebar.info("💡 Demo verisi gösteriliyor. Pipeline çalıştırıldığında gerçek veri gelecek.")
    np.random.seed(42)
    n = 5000
    df = pd.DataFrame({
        "trip_distance": np.random.uniform(0.5, 20.0, n),
        "pickup_hour": np.random.randint(0, 24, n),
        "pickup_day_of_week": np.random.randint(0, 7, n),
        "passenger_count": np.random.randint(1, 5, n),
    })
    df["fare_amount"] = (
        3.0
        + df["trip_distance"] * 2.5
        + df["pickup_hour"] * 0.5
        + np.random.normal(0, 2, n)
    )
    return df


df = load_data()

# Hedef kolon adını normalize et
target_col = "fare_amount" if "fare_amount" in df.columns else "label"

# ─────────────────────────────────────────────────────────
# SEKMELER
# ─────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(
    ["📊 Keşifsel Veri Analizi (EDA)", "🤖 Model Karşılaştırmaları", "📈 Regresyon Analizleri"]
)

# ─────────────────────────────────────────────────────────
# SEKME 1: EDA
# ─────────────────────────────────────────────────────────
with tab1:
    st.header("Veri Dağılımı ve Trendler")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Yolculuk Mesafesi Dağılımı")
        fig, ax = plt.subplots()
        sns.histplot(df["trip_distance"], bins=30, kde=True, ax=ax, color="skyblue")
        ax.set_xlabel("Mesafe (Mil)")
        st.pyplot(fig)
        plt.close()

        st.subheader("Yolcu Sayısı Dağılımı")
        fig, ax = plt.subplots()
        df["passenger_count"].value_counts().plot.pie(
            autopct="%1.1f%%", ax=ax, cmap="Pastel1"
        )
        ax.set_ylabel("")
        st.pyplot(fig)
        plt.close()

    with col2:
        st.subheader("Saatlik Ortalama Ücret Trendi")
        hourly_fare = df.groupby("pickup_hour")[target_col].mean()
        fig, ax = plt.subplots()
        ax.plot(hourly_fare.index, hourly_fare.values, marker="o", color="coral")
        ax.set_xlabel("Günün Saati (0-23)")
        ax.set_ylabel("Ortalama Ücret ($)")
        st.pyplot(fig)
        plt.close()

        st.subheader("Haftanın Günlerine Göre Yolculuklar")
        fig, ax = plt.subplots()
        sns.countplot(x="pickup_day_of_week", data=df, palette="viridis", ax=ax)
        ax.set_xlabel("Haftanın Günü (0=Pzt, 6=Paz)")
        st.pyplot(fig)
        plt.close()

    st.markdown("---")
    st.subheader("🔍 Eksik Değer Analizi")
    missing_data = df.isnull().sum().reset_index()
    missing_data.columns = ["Kolonlar", "Eksik Değer Sayısı"]

    col_miss1, col_miss2 = st.columns([1, 2])
    with col_miss1:
        st.dataframe(missing_data, hide_index=True, use_container_width=True)
        if missing_data["Eksik Değer Sayısı"].sum() == 0:
            st.success("Tüm eksik veriler Delta Lake Silver/Gold katmanlarında temizlendi!")
        else:
            st.warning("Veri setinde eksik değerler bulunuyor.")

    with col_miss2:
        fig_missing, ax_missing = plt.subplots(figsize=(8, 3))
        sns.barplot(
            x="Kolonlar",
            y="Eksik Değer Sayısı",
            data=missing_data,
            palette="Reds",
            ax=ax_missing,
        )
        plt.xticks(rotation=45, ha="right")
        ax_missing.set_ylabel("Null Kayıt Sayısı")
        st.pyplot(fig_missing)
        plt.close()

# ─────────────────────────────────────────────────────────
# SEKME 2: MODEL KARŞILAŞTIRMALARI
# ─────────────────────────────────────────────────────────
with tab2:
    st.header("Makine Öğrenmesi Model Performansları")
    st.markdown(
        "Eğitilen regresyon modellerinin MLflow'dan çekilen **gerçek zamanlı** metrikleri."
    )

    mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(mlflow_uri)

    metrics_df = pd.DataFrame()
    models_list = []

    try:
        experiment = mlflow.get_experiment_by_name("NYC_Taxi_Fare_Prediction")
        if experiment:
            runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
            if not runs.empty and "params.model_type" in runs.columns:
                latest_runs = runs.sort_values("start_time", ascending=False).drop_duplicates(
                    subset=["params.model_type"]
                )
                _models, _scores, _metrics = [], [], []
                for _, row in latest_runs.iterrows():
                    m = row["params.model_type"]
                    _models.extend([m, m, m])
                    _scores.extend([
                        row.get("metrics.r2_score", 0),
                        row.get("metrics.rmse", 0),
                        row.get("metrics.mae", 0),
                    ])
                    _metrics.extend(["R2 (↑ iyi)", "RMSE (↓ iyi)", "MAE (↓ iyi)"])
                metrics_df = pd.DataFrame({"Model": _models, "Skor": _scores, "Metrik": _metrics})
                models_list = list(dict.fromkeys(_models))
    except Exception:
        st.warning("MLflow'a bağlanılamadı. Henüz model eğitilmemiş olabilir.")

    if metrics_df.empty:
        st.info("💡 Varsayılan (örnek) metrikler gösteriliyor. Modeller eğitildikten sonra burası güncellenecek.")
        models_list = [
            "Linear_Regression", "Decision_Tree", "Random_Forest",
            "Gradient_Boosted_Trees", "Generalized_Linear_Ridge",
        ]
        metrics_df = pd.DataFrame({
            "Model": models_list * 3,
            "Skor": [0.85, 0.91, 0.94, 0.95, 0.86] + [1.2, 0.9, 0.5, 0.4, 1.1] + [0.99, 0.96, 0.98, 0.99, 0.99],
            "Metrik": ["R2 (↑ iyi)"] * 5 + ["RMSE (↓ iyi)"] * 5 + ["MAE (↓ iyi)"] * 5,
        })

    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(x="Model", y="Skor", hue="Metrik", data=metrics_df, palette="viridis", ax=ax)
    ax.set_ylabel("Metrik Değeri")
    plt.title("Modellerin Regresyon Metriklerine Göre Karşılaştırması")
    plt.xticks(rotation=20)
    st.pyplot(fig)
    plt.close()

    st.subheader("🌟 En Etkili Özellikler (Feature Importance)")
    fi_path = "/app/temp_plots/Random_Forest_feature_importance.png"
    if os.path.exists(fi_path):
        st.image(fi_path, caption="Fiyata en çok etki eden değişkenler", width=700)
    else:
        st.info("Feature Importance grafiği henüz yok. Model eğitimi tamamlandıktan sonra buraya yüklenecek.")

# ─────────────────────────────────────────────────────────
# SEKME 3: REGRESYON ANALİZLERİ
# ─────────────────────────────────────────────────────────
with tab3:
    st.header("Model Detay Analizleri")

    if not models_list:
        st.info("Model listesi yüklenemedi. Önce model eğitimini çalıştırın.")
    else:
        selected_model = st.selectbox("Model seçiniz:", list(dict.fromkeys(models_list)))
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Gerçek vs Tahmin (Scatter Plot)")
            scatter_path = f"/app/temp_plots/{selected_model}_scatter.png"
            if os.path.exists(scatter_path):
                st.image(scatter_path, use_container_width=True)
            else:
                st.info("Scatter plot henüz yok. Model eğitimi tamamlandıktan sonra görünecek.")

        with col2:
            st.subheader("Residual (Artık) Dağılımı")
            residual_path = f"/app/temp_plots/{selected_model}_residual.png"
            if os.path.exists(residual_path):
                st.image(residual_path, use_container_width=True)
            else:
                st.info("Residual plot henüz yok. Model eğitimi tamamlandıktan sonra görünecek.")
