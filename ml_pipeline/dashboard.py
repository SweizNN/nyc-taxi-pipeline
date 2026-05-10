import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Sayfa ayarları - Tasarım estetiği için geniş mod (PDF İsteri)
st.set_page_config(page_title="NYC Taxi Fiyat Tahmini", layout="wide")

st.title("🚖 NYC Yellow Taxi - Büyük Veri & ML Dashboard")
st.markdown(
    "Bu dashboard, Apache Kafka, Spark, Delta Lake ve MLflow kullanılarak oluşturulan uçtan uca veri hattının sonuçlarını göstermektedir.")


# --- 1. VERİ YÜKLEME (EDA İÇİN) ---
@st.cache_data
def load_data():
    # NOT: Gerçek veri gelince burayı pd.read_parquet("arkadasinin_verisi.parquet") yapacağız.
    # Şimdilik model dosyamızdaki gibi test verisi üretiyoruz.
    np.random.seed(42)
    n_samples = 5000
    df = pd.DataFrame({
        'trip_distance': np.random.uniform(0.5, 20.0, n_samples),
        'pickup_hour': np.random.randint(0, 24, n_samples),
        'day_of_week': np.random.randint(0, 7, n_samples),
        'passenger_count': np.random.randint(1, 5, n_samples),
    })
    df['fare_amount'] = 3.0 + (df['trip_distance'] * 2.5) + (df['pickup_hour'] * 0.5) + np.random.normal(0, 2,
                                                                                                         n_samples)
    return df


df = load_data()

# Düzenli görünüm için sekmeler (Tabs)
tab1, tab2, tab3 = st.tabs(["📊 Keşifsel Veri Analizi (EDA)", "🤖 Model Karşılaştırmaları", "📈 Regresyon Analizleri"])

# --- SEKME 1: EDA VE VERİ DAĞILIMI (PDF ZORUNLU İSTERİ) ---
with tab1:
    st.header("Veri Dağılımı ve Trendler")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Yolculuk Mesafesi Dağılımı (Histogram)")
        fig, ax = plt.subplots()
        sns.histplot(df['trip_distance'], bins=30, kde=True, ax=ax, color='skyblue')
        ax.set_xlabel("Mesafe (Mil)")
        ax.set_ylabel("Frekans")
        st.pyplot(fig)

        st.subheader("Yolcu Sayısı Dağılımı (Pie Chart)")
        fig, ax = plt.subplots()
        df['passenger_count'].value_counts().plot.pie(autopct='%1.1f%%', ax=ax, cmap='Pastel1')
        ax.set_ylabel("")
        st.pyplot(fig)

    with col2:
        st.subheader("Saatlik Ortalama Ücret Trendi (Line Chart)")
        hourly_fare = df.groupby('pickup_hour')['fare_amount'].mean()
        fig, ax = plt.subplots()
        ax.plot(hourly_fare.index, hourly_fare.values, marker='o', color='coral')
        ax.set_xlabel("Günün Saati (0-23)")
        ax.set_ylabel("Ortalama Ücret ($)")
        st.pyplot(fig)

        st.subheader("Haftanın Günlerine Göre Yolculuklar (Bar Chart)")
        fig, ax = plt.subplots()
        sns.countplot(x='day_of_week', data=df, palette='viridis', ax=ax)
        ax.set_xlabel("Haftanın Günü (0=Pzt, 6=Paz)")
        st.pyplot(fig)

# --- SEKME 2: MODEL KARŞILAŞTIRMALARI (PDF ZORUNLU İSTERİ) ---
with tab2:
    st.header("Makine Öğrenmesi Model Performansları")
    st.markdown("Proje kapsamında eğitilen 5 farklı regresyon modelinin performans karşılaştırması.")

    # Not: Gerçek projede MLflow API'sinden çekilir, pratiklik için MLflow'daki sonuçları buraya manuel bar chart olarak basıyoruz.
    models_list = ["Linear_Regression", "Decision_Tree", "Random_Forest", "Gradient_Boosted_Trees",
                   "Generalized_Linear_Ridge"]
    r2_scores = [0.99, 0.96, 0.98, 0.99, 0.99]  # Rastgele veriyle ürettiğimiz yaklaşık R2 skorları

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.barplot(x=models_list, y=r2_scores, palette="mako", ax=ax)
    ax.set_ylabel("R2 Skoru (1'e yakın olan daha iyidir)")
    ax.set_ylim(0, 1.1)
    plt.xticks(rotation=15)
    st.pyplot(fig)

    st.subheader("🌟 En Etkili Özellikler (Feature Importance)")
    # Modelin kaydettiği grafiği çekiyoruz
    fi_path = "temp_plots/Random_Forest_feature_importance.png"
    if os.path.exists(fi_path):
        st.image(fi_path, caption="Random Forest modeline göre fiyata en çok etki eden değişkenler", width=700)
    else:
        st.warning("Feature Importance grafiği bulunamadı. Lütfen önce model_training.py dosyasını çalıştırın.")

# --- SEKME 3: REGRESYON ANALİZLERİ (PDF ZORUNLU İSTERİ) ---
with tab3:
    st.header("Model Detayları (Gerçek vs Tahmin & Artıklar)")
    selected_model = st.selectbox("Detaylarını görmek istediğiniz modeli seçin:", models_list)

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
