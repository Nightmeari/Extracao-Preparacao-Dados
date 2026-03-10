import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# -----------------------------
# DADOS (colocados manualmente)
# -----------------------------

times = [
    "Palmeiras",
    "São Paulo",
    "Corinthians",
    "Bahia",
    "Fluminense",
    "Athletico-PR"
]

pontos = [10, 10, 7, 7, 7, 7]


# -----------------------------
# GRÁFICO
# -----------------------------

fig, ax = plt.subplots(figsize=(10,7), facecolor="#0f0f1a")
ax.set_facecolor("#0f0f1a")

cores = ["#7B61FF","#00D4AA","#FF6B6B","#FFD166","#06D6A0","#118AB2"]

wedges, _, autotexts = ax.pie(
    pontos,
    autopct="%1.1f%%",
    startangle=90,
    colors=cores,
    wedgeprops=dict(width=0.55, edgecolor="#0f0f1a", linewidth=2.5),
    pctdistance=0.78,
    textprops={"color":"white","fontsize":11,"fontweight":"bold"}
)

ax.text(0,0.10,"Top 6",ha="center",fontsize=16,fontweight="bold",color="white")
ax.text(0,-0.12,"Times\npor Pontos",ha="center",fontsize=10,color="#aaaaaa")


# -----------------------------
# LEGENDA
# -----------------------------

legenda = []

for i, time in enumerate(times):

    legenda.append(
        mpatches.Patch(
            color=cores[i],
            label=f"{time} — {pontos[i]} pts"
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