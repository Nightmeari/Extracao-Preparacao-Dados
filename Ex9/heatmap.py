import pandas as pd
import plotly.express as px

# ─────────────────────────────
# 1. Carregar dados
# ─────────────────────────────
df = pd.read_csv(r"Extração/Extracao-Preparacao-Dados/Ex9/climatempo_weatherlatlong.csv")

# ─────────────────────────────
# 2. Limpeza
# ─────────────────────────────
df = df.dropna(subset=["temperature", "latitude", "longitude"])

# opcional: filtrar Brasil só pra garantir
df = df[df["state"].notna()]

# ─────────────────────────────
# 3. Heatmap geográfico
# ─────────────────────────────
fig = px.density_mapbox(
    df,
    lat="latitude",
    lon="longitude",
    z="temperature",              # intensidade (calor)
    radius=5,                   # espalhamento (ajuste aqui!)
    center=dict(lat=-14, lon=-55),  # centro do Brasil
    zoom=3,
    mapbox_style="carto-positron",
    color_continuous_scale="RdYlBu_r",
    title="🔥 Heatmap de Temperatura no Brasil"
)

fig.show()