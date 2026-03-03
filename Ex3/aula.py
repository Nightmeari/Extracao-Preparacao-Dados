import pandas as pd
import re

caminho = r"C:\Users\marii\OneDrive\Desktop\extracao\Extração\Extracao-Preparacao-Dados\Ex3\mises_citations_fill_page.xlsx - Sheet1.csv"
saida   = r"C:\Users\marii\OneDrive\Desktop\extracao\Extração\Extracao-Preparacao-Dados\Ex3\resultado.csv"

df = pd.read_csv(caminho)

# 1) p. 12 / pp. 12-15 / pp.12 (pega só o primeiro número)
RE_P = re.compile(r'\bpp?\.\s*(\d+)', re.IGNORECASE)

# 2) ano: 12-15 / 1944: 12 / 1944: 12-15 (pega só o primeiro número depois de :)
RE_COLON = re.compile(r':\s*(\d+)(?:\s*-\s*\d+)?\b')

# 3) ... , 12-15) no final (pega só o primeiro número depois da última vírgula perto do fim)
RE_END = re.compile(r',\s*(\d+)(?:\s*-\s*\d+)?\s*\)?\s*$')

# 4) fallback: pega todos os números/intervalos, mas vamos filtrar anos
RE_ANY = re.compile(r'\b(\d+)(?:-\d+)?\b')

def eh_ano(n: int) -> bool:
    # ajuste o range se precisar
    return 1500 <= n <= 2099

def extrair_pagina(texto):
    if pd.isna(texto):
        return pd.NA

    s = str(texto)

    # A) Se tem p./pp., é o sinal mais confiável
    m = RE_P.search(s)
    if m:
        n = int(m.group(1))
        return pd.NA if eh_ano(n) else n

    # B) Padrão com ":" (muito comum em citações)
    m = RE_COLON.search(s)
    if m:
        n = int(m.group(1))
        return pd.NA if eh_ano(n) else n

    # C) Número no final depois de vírgula
    m = RE_END.search(s)
    if m:
        n = int(m.group(1))
        return pd.NA if eh_ano(n) else n

    # D) Fallback (mantém sua ideia de "pegar o último"), mas filtrando anos
    nums = [int(x) for x in RE_ANY.findall(s)]
    nums = [n for n in nums if not eh_ano(n)]
    if nums:
        return nums[-1]

    return pd.NA

df["paginas"] = df["raw"].apply(extrair_pagina).astype("Int64")  # Int64 evita 1.0 e permite vazio

df.to_csv(saida, index=False)
print("Pronto! Arquivo salvo como:", saida)