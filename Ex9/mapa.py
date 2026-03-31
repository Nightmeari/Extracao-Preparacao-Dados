import json
import pandas as pd
import plotly.express as px

df = pd.read_csv(r"Extração\Extracao-Preparacao-Dados\Ex9\climatempo_weather.csv")
df = df.dropna(subset=["temperature"])

df_state = df.groupby("state")["temperature"].mean().reset_index()

# carregar geojson
with open(r"Extração\Extracao-Preparacao-Dados\Ex9\brazil-states.geojson") as f:
    geojson = json.load(f)

fig = px.choropleth(
    df_state,
    geojson=geojson,
    locations="state",
    featureidkey="properties.sigla",
    color="temperature",
    color_continuous_scale="RdYlBu_r",
    title="Temperatura Média por Estado (Brasil)"
)

fig.update_geos(fitbounds="locations", visible=False)
fig.show()