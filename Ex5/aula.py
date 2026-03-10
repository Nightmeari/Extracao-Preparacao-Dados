import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import re
from playwright.sync_api import sync_playwright


# -----------------------------
# SCRAPING
# -----------------------------

def coletar_dados():

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=True)

        page = browser.new_page()

        print("Acessando o site...")

        page.goto("https://www.fundsexplorer.com.br/ranking")

        # esperar tabela aparecer
        page.wait_for_selector("table")

        # esperar JS preencher a tabela
        page.wait_for_timeout(6000)

        headers = page.eval_on_selector_all(
            "thead th",
            "els => els.map(e => e.innerText.trim())"
        )

        rows = page.eval_on_selector_all(
            "tbody tr",
            """rows => rows.map(row =>
                Array.from(row.querySelectorAll('td')).map(td => td.innerText.trim())
            )"""
        )

        browser.close()

        return headers, rows


headers, rows = coletar_dados()

print("Linhas coletadas:", len(rows))

df = pd.DataFrame(rows, columns=headers)

df = df[df.apply(lambda row: any(cell != "" for cell in row), axis=1)]

df.to_csv("fundos_imobiliarios.csv", index=False, encoding="utf-8-sig")

print(f"CSV salvo com {len(df)} fundos")


# -----------------------------
# LIMPEZA
# -----------------------------

def limpar_valor(v):

    if pd.isna(v):
        return np.nan

    v = re.sub(r"[R$\s.]", "", str(v)).replace(",", ".")

    try:
        return float(v)

    except:
        return np.nan


df["PL_NUM"] = df["PATRIMÔNIO LÍQUIDO"].apply(limpar_valor)


# -----------------------------
# ANÁLISE
# -----------------------------

top6 = (
    df.dropna(subset=["PL_NUM"])
    .groupby("SETOR")["PL_NUM"]
    .sum()
    .nlargest(6)
    .reset_index()
)

print("\nTop 6 setores por patrimônio líquido:\n")
print(top6)


# -----------------------------
# GRÁFICO
# -----------------------------

fig, ax = plt.subplots(figsize=(10,7), facecolor="#0f0f1a")

ax.set_facecolor("#0f0f1a")

cores = ["#7B61FF","#00D4AA","#FF6B6B","#FFD166","#06D6A0","#118AB2"]

wedges, _, autotexts = ax.pie(
    top6["PL_NUM"],
    autopct="%1.1f%%",
    startangle=90,
    colors=cores,
    wedgeprops=dict(width=0.55, edgecolor="#0f0f1a", linewidth=2.5),
    pctdistance=0.78,
    textprops={"color":"white","fontsize":11,"fontweight":"bold"}
)

ax.text(0,0.10,"Top 6",ha="center",fontsize=16,fontweight="bold",color="white")

ax.text(0,-0.12,"Patrimônio\nLíquido",ha="center",fontsize=10,color="#aaaaaa")


legenda=[]

for i,row in top6.iterrows():

    val=row["PL_NUM"]

    if val>=1e9:
        val_fmt=f"R$ {val/1e9:.2f}B"
    else:
        val_fmt=f"R$ {val/1e6:.0f}M"

    legenda.append(
        mpatches.Patch(
            color=cores[i],
            label=f"{row['SETOR']} — {val_fmt}"
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
    "Top 6 Setores por Patrimônio Líquido",
    color="white",
    fontsize=16,
    fontweight="bold",
    pad=20
)

plt.tight_layout()

plt.savefig(
    "rosca_top6_setor_pl.png",
    dpi=150,
    bbox_inches="tight",
    facecolor="#0f0f1a"
)

plt.show()

print("\nGráfico salvo como 'rosca_top6_setor_pl.png'")