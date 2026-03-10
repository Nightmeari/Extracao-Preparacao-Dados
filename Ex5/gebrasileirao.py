import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from playwright.sync_api import sync_playwright


# -----------------------------
# SCRAPING
# -----------------------------

def coletar_dados():

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=True)

        page = browser.new_page()

        print("Acessando o site...")

        page.goto(
            "https://ge.globo.com/futebol/brasileirao-serie-a/",
            wait_until="domcontentloaded",
            timeout=60000
        )

        page.wait_for_selector("table")

        page.wait_for_timeout(5000)

        rows = page.eval_on_selector_all(
            "tbody tr",
            """rows => rows.map(row =>
                Array.from(row.querySelectorAll('td')).map(td => td.innerText.trim())
            )"""
        )

        browser.close()

        return rows


rows = coletar_dados()

print("Linhas coletadas:", len(rows))


# -----------------------------
# DATAFRAME
# -----------------------------

df = pd.DataFrame(rows)

df.columns = [
    "POS","TIME","PTS","J","V","E","D","GP","GC","SG"
]


# remover linhas quebradas
df = df[df["TIME"].str.contains("[A-Za-z]", na=False)]


# manter apenas os 20 times da tabela
df = df.head(20)


# salvar csv
df.to_csv("brasileirao_classificacao.csv", index=False, encoding="utf-8-sig")

print(f"CSV salvo com {len(df)} times")


# -----------------------------
# LIMPEZA
# -----------------------------

df["PTS_NUM"] = pd.to_numeric(df["PTS"], errors="coerce")


# -----------------------------
# ANÁLISE
# -----------------------------

top6 = df.sort_values("PTS_NUM", ascending=False).head(6)

print("\nTop 6 times por pontos:\n")
print(top6[["TIME","PTS_NUM"]])


# -----------------------------
# GRÁFICO
# -----------------------------

fig, ax = plt.subplots(figsize=(10,7), facecolor="#0f0f1a")

ax.set_facecolor("#0f0f1a")

cores = ["#7B61FF","#00D4AA","#FF6B6B","#FFD166","#06D6A0","#118AB2"]

wedges, _, autotexts = ax.pie(
    top6["PTS_NUM"],
    autopct="%1.1f%%",
    startangle=90,
    colors=cores,
    wedgeprops=dict(width=0.55, edgecolor="#0f0f1a", linewidth=2.5),
    pctdistance=0.78,
    textprops={"color":"white","fontsize":11,"fontweight":"bold"}
)

ax.text(0,0.10,"Top 6",ha="center",fontsize=16,fontweight="bold",color="white")

ax.text(0,-0.12,"Times\npor Pontos",ha="center",fontsize=10,color="#aaaaaa")


legenda=[]

for i,(idx,row) in enumerate(top6.iterrows()):

    val=row["PTS_NUM"]

    legenda.append(
        mpatches.Patch(
            color=cores[i],
            label=f"{row['TIME']} — {val} pts"
        )
    )


ax.legend(
    handles=legenda,
    loc="lower center",
    bbox_to_anchor=(0.5,-0.18),
    ncol=2,
    frameon=False,
    fontsize=10,
    labelcolor="white"
)


ax.set_title(
    "Top 6 Times do Brasileirão por Pontos",
    color="white",
    fontsize=16,
    fontweight="bold",
    pad=20
)


plt.tight_layout()

plt.savefig(
    "rosca_top6_brasileirao_pontos.png",
    dpi=150,
    bbox_inches="tight",
    facecolor="#0f0f1a"
)

plt.show()

print("\nGráfico salvo como 'rosca_top6_brasileirao_pontos.png'")