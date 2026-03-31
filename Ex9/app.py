import streamlit as st
import pandas as pd
import plotly.express as px

# ─────────────────────────────
# CONFIG
# ─────────────────────────────
st.set_page_config(layout="wide")
st.title("🔥 Mapa de Temperatura do Brasil")

# ─────────────────────────────
# CARREGAR DADOS
# ─────────────────────────────
df = pd.read_csv("climatempo_enriched.csv")

# ─────────────────────────────
# FILTROS
# ─────────────────────────────
col1, col2 = st.columns(2)

with col1:
    state = st.selectbox("Estado", ["Todos"] + sorted(df["state"].unique()))

with col2:
    metric = st.selectbox(
        "Métrica",
        ["temperature", "feels_like", "diff", "cluster"]
    )

# filtrar estado
if state != "Todos":
    df = df[df["state"] == state]

# ─────────────────────────────
# MAPA HEATMAP
# ─────────────────────────────
fig = px.density_mapbox(
    df,
    lat="latitude",
    lon="longitude",
    z=metric if metric != "cluster" else None,
    radius=25,
    center=dict(lat=-14, lon=-55),
    zoom=3 if state == "Todos" else 5,
    mapbox_style="carto-positron",
    title=f"Mapa de {metric}"
)

st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────
# MAPA DE CLUSTERS (BONUS)
# ─────────────────────────────
st.subheader("📍 Regiões térmicas (clustering)")

fig2 = px.scatter_mapbox(
    df,
    lat="latitude",
    lon="longitude",
    color="cluster",
    hover_name="city",
    zoom=3 if state == "Todos" else 5,
    mapbox_style="carto-positron",
    title="Clusters de temperatura"
)

st.plotly_chart(fig2, use_container_width=True)

# ─────────────────────────────
# INSIGHTS RÁPIDOS
# ─────────────────────────────
st.subheader("📊 Insights")

col3, col4, col5 = st.columns(3)

with col3:
    st.metric("Temp média", round(df["temperature"].mean(), 1))

with col4:
    st.metric("Sensação média", round(df["feels_like"].mean(), 1))

with col5:
    st.metric("Diferença média", round(df["diff"].mean(), 1))