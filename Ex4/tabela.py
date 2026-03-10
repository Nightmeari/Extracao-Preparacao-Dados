import requests
import pandas as pd
from bs4 import BeautifulSoup

url = "https://www.mat.ufmg.br/futebol/campeao_seriea/"

# baixar página
headers = {"User-Agent": "Mozilla/5.0"}
html = requests.get(url, headers=headers).text

# parsear html
soup = BeautifulSoup(html, "html.parser")

# pegar linhas da tabela
linhas = soup.find_all("tr")

dados = []

for linha in linhas:
    colunas = linha.find_all("td")
    if len(colunas) == 3:
        pos = colunas[0].text.strip()
        time = colunas[1].text.strip()
        prob = colunas[2].text.strip()
        dados.append([pos, time, prob])

# dataframe
df = pd.DataFrame(dados, columns=["posicao", "time", "probabilidade_campeao"])

# salvar csv
df.to_csv("prob_campeao_seriea.csv", index=False, encoding="utf-8")

print(df.head())