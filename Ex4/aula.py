import pandas as pd
import requests

url = "https://en.wikipedia.org/wiki/List_of_countries_with_McDonald%27s_restaurants"

headers = {"User-Agent": "Mozilla/5.0"}

response = requests.get(url, headers=headers)

tabelas = pd.read_html(response.text)

print(f"Número de tabelas encontradas: {len(tabelas)}")

df_mcdonalds_por_pais = tabelas[0]

print(df_mcdonalds_por_pais.head())

df_mcdonalds_por_pais.to_csv("mcdonalds_por_pais.csv", index=False)

print(df_mcdonalds_por_pais.info())