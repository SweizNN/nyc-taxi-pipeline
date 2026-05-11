import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import mlflow

# Sayfa ayarları - Tasarım estetiği için geniş mod 
st.set_page_config(page_title="NYC Taxi Fiyat Tahmini", layout="wide")

st.title("🚖 NYC Yellow Taxi - Büyük Veri & ML Dashboard")
st.markdown(
    "Bu dashboard; Kafka, Spark ve Delta Lake üzerinden akan verilerin analizini ve MLflow ile takip edilen model sonuçlarını sunar."
)

# --- 1. VERİ YÜKLEME (EĞİTİM KODU İLE TAM UYUMLU) ---
@st.cache_resource # SparkSession'ı bir kez oluşturup saklamak için
def get_spark():
    builder = SparkSession.builder.appName("NYC_Taxi_Dashboard") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4")
    return configure_spark_with_delta_pip(builder).getOrCreate()

@st.cache_data
def load_data_from_gold():
    gold_path = os.getenv("DELTA_GOLD_PATH", "/app/data/delta/gold/fare_features")
    try:
        spark = get_spark()
        df_spark = spark.read.format("delta").load(gold_path)
        return df_spark.toPandas()
    except Exception as e:
        st.sidebar.error(f"Gerçek veri okunamadı (Test verisi yükleniyor): {e}")
        np.random.seed(42)
        n_samples = 5000
        df = pd.DataFrame({
            'trip_distance': np.random.uniform(0.5, 20.0, n_samples),
            'pickup_hour': np.random.randint(0, 24, n_samples),
            'pickup_day_of_week': np.random.randint(0, 7, n_samples),
            'passenger_count': np.random.randint(1, 5, n_samples),
        })
        df['label'] = 3.0 + (df['trip_distance'] * 2.5) + (df['pickup_hour'] * 0.5) + np.random.normal(0, 2, n_samples)
        return df

df = load_data_from_gold()

# Kolon ismi kontrolü (label veya fare_amount)
target_col = 'label' if 'label' in df.columns else 'fare_amount'

# Düzenli görünüm için sekmeler (Tabs)
tab1, tab2, tab3 = st.tabs(["📊 Keşifsel Veri Analizi (EDA)", "🤖 Model Karşılaştırmaları", "📈 Regresyon Analizleri"])

# --- SEKME 1: EDA VE VERİ DAĞILIMI  ---
with tab1:
    st.header("Veri Dağılımı ve Trendler")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Yolculuk Mesafesi Dağılımı (Histogram)")
        fig, ax = plt.subplots()
        sns.histplot(df['trip_distance'], bins=30, kde=True, ax=ax, color='skyblue')
        ax.set_xlabel("Mesafe (Mil)")
        st.pyplot(fig)

        st.subheader("Yolcu Sayısı Dağılımı (Pie Chart)")
        fig, ax = plt.subplots()
        df['passenger_count'].value_counts().plot.pie(autopct='%1.1f%%', ax=ax, cmap='Pastel1')
        ax.set_ylabel("")
        st.pyplot(fig)

    with col2:
        st.subheader("Saatlik Ortalama Ücret Trendi (Line Chart)")
        hourly_fare = df.groupby('pickup_hour')[target_col].mean()
        fig, ax = plt.subplots()
        ax.plot(hourly_fare.index, hourly_fare.values, marker='o', color='coral')
        ax.set_xlabel("Günün Saati (0-23)")
        ax.set_ylabel("Ortalama Ücret ($)")
        st.pyplot(fig)

        st.subheader("Haftanın Günlerine Göre Yolculuklar (Bar Chart)")
        fig, ax = plt.subplots()
        # pickup_day_of_week kullanımı (model_training.py ile uyumlu)
        sns.countplot(x='pickup_day_of_week', data=df, palette='viridis', ax=ax)
        ax.set_xlabel("Haftanın Günü (0=Pzt, 6=Paz)")
        st.pyplot(fig)

    st.markdown("---")
    st.subheader("🔍 Eksik Değer Analizi (Missing Values)")
    missing_data = df.isnull().sum().reset_index()
    missing_data.columns = ['Kolonlar', 'Eksik Değer Sayısı']

    col_miss1, col_miss2 = st.columns([1, 2])
    with col_miss1:
        # Streamlit 1.23+ versiyonları hide_index parametresini destekler
        st.dataframe(missing_data, hide_index=True, use_container_width=True)
        if missing_data['Eksik Değer Sayısı'].sum() == 0:
            st.success("Tüm eksik veriler (Null değerler) Delta Lake Silver/Gold katmanlarında başarıyla temizlenmiş!")
        else:
            st.warning("Veri setinde eksik değerler bulunuyor.")
            
    with col_miss2:
        fig_missing, ax_missing = plt.subplots(figsize=(8, 3))
        sns.barplot(x='Kolonlar', y='Eksik Değer Sayısı', data=missing_data, palette="Reds", ax=ax_missing)
        plt.xticks(rotation=45, ha='right')
        ax_missing.set_ylabel("Null Kayıt Sayısı")
        st.pyplot(fig_missing)

# --- SEKME 2: MODEL KARŞILAŞTIRMALARI ---
with tab2:
    st.header("Makine Öğrenmesi Model Performansları")
    st.markdown("Eğitilen regresyon modellerinin MLflow'dan çekilen **gerçek zamanlı** metrikleri üzerinden karşılaştırması.")

    # MLflow'dan dinamik veri çekme
    mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(mlflow_uri)

    metrics_df = pd.DataFrame()
    try:
        experiment = mlflow.get_experiment_by_name("NYC_Taxi_Fare_Prediction")
        if experiment:
            runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
            if not runs.empty and 'params.model_type' in runs.columns:
                # En güncel run'ları almak için tarihe göre sırala ve modele göre tekilleştir
                latest_runs = runs.sort_values("start_time", ascending=False).drop_duplicates(subset=["params.model_type"])
                
                models_list, skorlar, metrikler = [], [], []
                for _, row in latest_runs.iterrows():
                    m_name = row['params.model_type']
                    models_list.extend([m_name, m_name, m_name])
                    skorlar.extend([row.get('metrics.r2_score', 0), row.get('metrics.rmse', 0), row.get('metrics.mae', 0)])
                    metrikler.extend(["R2 (Yüksek Daha İyi)", "RMSE (Düşük Daha İyi)", "MAE (Düşük Daha İyi)"])
                    
                metrics_df = pd.DataFrame({"Model": models_list, "Skor": skorlar, "Metrik": metrikler})
    except Exception as e:
        st.warning("MLflow verileri çekilemedi. Henüz model eğitilmemiş olabilir.")

    # Eğer MLflow'dan veri gelmezse (örneğin ilk çalıştırma öncesi) çökmeyi önlemek için varsayılan/örnek veriyi göster
    if metrics_df.empty:
        st.info("💡 Şu an ekranda varsayılan (örnek) model metriklerini görüyorsunuz. Modeller eğitildiğinde burası otomatik güncellenecektir.")
        models_list = ["Linear_Regression", "Decision_Tree", "Random_Forest", "Gradient_Boosted_Trees", "Generalized_Linear_Ridge"]
        metrics_df = pd.DataFrame({
            "Model": models_list * 3,
            "Skor": [0.85, 0.91, 0.94, 0.95, 0.86] + [1.2, 0.9, 0.5, 0.4, 1.1] + [0.99, 0.96, 0.98, 0.99, 0.99],
            "Metrik": ["R2 (Yüksek Daha İyi)"] * 5 + ["RMSE (Düşük Daha İyi)"] * 5 + ["MAE (Düşük Daha İyi)"] * 5
        })

    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(x="Model", y="Skor", hue="Metrik", data=metrics_df, palette="viridis", ax=ax)
    ax.set_ylabel("Metrik Değeri")
    plt.title("Modellerin Regresyon Metriklerine Göre Karşılaştırması (Grouped Bar Chart)")
    plt.xticks(rotation=20)
    st.pyplot(fig)

    st.subheader("🌟 En Etkili Özellikler (Feature Importance)")
    fi_path = "temp_plots/Random_Forest_feature_importance.png"
    if os.path.exists(fi_path):
        st.image(fi_path, caption="Modele göre fiyata en çok etki eden değişkenler", width=700)
    else:
        st.warning("Feature Importance grafiği henüz oluşturulmadı. Lütfen önce eğitim kodunu çalıştırın.")

# --- SEKME 3: REGRESYON ANALİZLERİ ---
with tab3:
    st.header("Model Detay Analizleri")
    
    unique_models = list(dict.fromkeys(models_list)) # Listedeki duplicate/tekrar eden model isimlerini temizle
    selected_model = st.selectbox("Model seçiniz:", unique_models)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Gerçek vs Tahmin (Scatter Plot)")
        scatter_path = f"temp_plots/{selected_model}_scatter.png"
        if os.path.exists(scatter_path):
            st.image(scatter_path, use_container_width=True)

    with col2:
        st.subheader("Residual (Artık) Dağılımı")
        residual_path = f"temp_plots/{selected_model}_residual.png"
        if os.path.exists(residual_path):
            st.image(residual_path, use_container_width=True)
