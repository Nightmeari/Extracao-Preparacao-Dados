import pandas as pd
from sklearn.cluster import KMeans

# carregar dados
df = pd.read_csv(r"Extração/Extracao-Preparacao-Dados/Ex9/climatempo_weatherlatlong.csv")

# limpar
df = df.dropna(subset=["temperature", "feels_like", "latitude", "longitude"])

# ─────────────────────────────
# FEATURE ENGINEERING
# ─────────────────────────────

# diferença sensação vs real
df["diff"] = df["feels_like"] - df["temperature"]

# ─────────────────────────────
# CLUSTERING (regiões térmicas)
# ─────────────────────────────

# usa lat/lon + temperatura
X = df[["latitude", "longitude", "temperature"]]

kmeans = KMeans(n_clusters=5, random_state=42)
df["cluster"] = kmeans.fit_predict(X)

# salvar dataset enriquecido
df.to_csv("climatempo_enriched.csv", index=False)

print("Dataset enriquecido salvo ✅")