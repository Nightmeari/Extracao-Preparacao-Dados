# no terminal escreva: python naosei.py 2025.findings-acl.656.pdf.tei.xml
import re
import csv
import argparse
from pathlib import Path

def split_sentences(text: str):
    """
    Quebra simples em frases (heurística).
    Se você quiser algo mais robusto, dá pra trocar por spaCy depois.
    """
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return []
    # separa após . ! ? seguidos de espaço e letra/abre aspas/parênteses
    parts = re.split(r'(?<=[.!?])\s+(?=[A-ZÁÀÂÃÉÊÍÓÔÕÚÜ"“(\[])', text)
    return [p.strip() for p in parts if p.strip()]

def get_localname(tag: str) -> str:
    """Remove namespace: '{...}p' -> 'p' """
    return tag.split("}", 1)[-1] if "}" in tag else tag

def format_biblstruct(bibl) -> str:
    """
    Monta uma referência curta tipo: 'Sobrenome et al., 2023 – Título...'
    (bem heurístico; TEI varia bastante)
    """
    def text_of(xpath_expr):
        el = bibl.find(xpath_expr)
        return "".join(el.itertext()).strip() if el is not None else ""

    # tenta pegar autor (sobrenome do primeiro)
    surname = ""
    first_author_surname = bibl.find(".//{*}author//{*}surname")
    if first_author_surname is not None and first_author_surname.text:
        surname = first_author_surname.text.strip()

    # ano
    year = ""
    date_el = bibl.find(".//{*}date")
    if date_el is not None:
        year = (date_el.get("when") or "".join(date_el.itertext())).strip()
        # se vier "2023-05-01", pega só o ano
        m = re.match(r"(\d{4})", year)
        if m:
            year = m.group(1)

    # título (prioriza analytic/title, senão monogr/title)
    title = ""
    title_el = bibl.find(".//{*}analytic//{*}title")
    if title_el is None:
        title_el = bibl.find(".//{*}monogr//{*}title")
    if title_el is not None:
        title = "".join(title_el.itertext()).strip()

    bits = []
    if surname and year:
        bits.append(f"{surname}, {year}")
    elif surname:
        bits.append(surname)
    elif year:
        bits.append(year)
    if title:
        bits.append(title)

    out = " – ".join(bits).strip()
    return out if out else None

def build_bibliography_map(root):
    """
    Mapeia xml:id -> string de referência curta (quando possível).
    Ex.: 'b12' -> 'Silva, 2023 – Title...'
    """
    bib_map = {}
    for bibl in root.findall(".//{*}biblStruct"):
        # xml:id pode vir como atributo namespaced
        xml_id = bibl.get("{http://www.w3.org/XML/1998/namespace}id") or bibl.get("id")
        if not xml_id:
            continue
        formatted = format_biblstruct(bibl)
        if formatted:
            bib_map[xml_id] = formatted
    return bib_map

def paragraph_text_with_refs(p_el):
    """
    Constrói o texto do parágrafo preservando o texto das refs (ex.: [12])
    e também devolve uma lista de ocorrências:
    [{'raw':'[12]', 'target':'b12', 'index':pos_no_texto_final}, ...]
    """
    pieces = []
    occurrences = []

    def walk(node):
        # texto antes de filhos
        if node.text:
            pieces.append(node.text)

        for child in list(node):
            name = get_localname(child.tag)

            if name == "ref" and (child.get("type") == "bibr" or "bibr" in (child.get("type") or "")):
                raw = "".join(child.itertext()).strip()
                target = (child.get("target") or "").lstrip("#").strip()  # "#b12" -> "b12"

                # marca posição antes de adicionar
                current_text = "".join(pieces)
                idx = len(current_text)

                pieces.append(raw)

                occurrences.append({
                    "raw": raw,
                    "target": target,
                    "index": idx,
                })

                if child.tail:
                    pieces.append(child.tail)
            else:
                # qualquer outro nó: inclui texto interno
                walk(child)
                if child.tail:
                    pieces.append(child.tail)

    walk(p_el)
    full_text = re.sub(r"\s+", " ", "".join(pieces)).strip()
    return full_text, occurrences

def find_sentence_containing(text: str, needle: str):
    """
    Retorna a frase (ou trecho) que contém 'needle'. Se não achar, retorna um window.
    """
    sentences = split_sentences(text)
    for s in sentences:
        if needle in s:
            return s

    # fallback: janela de caracteres
    pos = text.find(needle)
    if pos == -1:
        return text[:300] if len(text) > 300 else text

    start = max(0, pos - 120)
    end = min(len(text), pos + len(needle) + 120)
    snippet = text[start:end].strip()
    if start > 0:
        snippet = "…" + snippet
    if end < len(text):
        snippet = snippet + "…"
    return snippet

def extract_rows(xml_path: Path):
    # tenta usar lxml (melhor), cai para xml.etree se não tiver
    try:
        from lxml import etree
        parser = etree.XMLParser(recover=True, huge_tree=True)
        tree = etree.parse(str(xml_path), parser)
        root = tree.getroot()
    except Exception:
        import xml.etree.ElementTree as ET
        tree = ET.parse(str(xml_path))
        root = tree.getroot()

    bib_map = build_bibliography_map(root)

    rows = []

    # pega todos os parágrafos
    for p in root.findall(".//{*}p"):
        p_text, occs = paragraph_text_with_refs(p)
        if not occs:
            continue

        for occ in occs:
            raw = occ["raw"] or ""
            target = occ["target"] or ""

            # decide o que vai na coluna "citação"
            # preferimos referência da bibliografia se existir, senão usa o raw ([12])
            citation = bib_map.get(target) if target in bib_map else raw
            if not citation:
                citation = raw or target or ""

            contexto = find_sentence_containing(p_text, raw) if raw else p_text

            # opcional: destacar a citação no contexto
            if raw and raw in contexto:
                contexto = contexto.replace(raw, f"<<{raw}>>")

            rows.append({
                "citação": citation,
                "contexto": contexto
            })

    return rows

def main():
    ap = argparse.ArgumentParser(description="Extrai citações (ref type=bibr) de TEI XML para CSV.")
    ap.add_argument("xml", help="Caminho do arquivo .tei.xml")
    ap.add_argument("-o", "--out", default="citacoes.csv", help="Arquivo CSV de saída (default: citacoes.csv)")
    args = ap.parse_args()

    xml_path = Path(args.xml)
    if not xml_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {xml_path}")

    rows = extract_rows(xml_path)

    out_path = Path(args.out)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["citação", "contexto"], quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(rows)

    print(f"OK: {len(rows)} linhas salvas em {out_path.resolve()}")

if __name__ == "__main__":
    xml_file = "Ex1/2025.findings-acl.656.pdf.tei.xml"
    output_file = "Ex1/citacoes.csv"

    rows = extract_rows(xml_file)

    import csv
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["citação", "contexto"])
        writer.writeheader()
        writer.writerows(rows)

    print("Pronto!")