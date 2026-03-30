import requests
import pandas as pd

token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IlFaZ045SHFOa0dORU00R2VLY3pEMDJQY1Z2NCIsImtpZCI6IlFaZ045SHFOa0dORU00R2VLY3pEMDJQY1Z2NCJ9.eyJhdWQiOiI3M2JjMTc4Ny1hZjhlLTQ0MGMtYTUwYS1jZWIyOTAzMGE2M2MiLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC9kYTQ5YTg0NC1lMmUzLTQwYWYtODZhNi1jMzgxOWQ3MDRmNDkvIiwiaWF0IjoxNzc0ODY4OTcxLCJuYmYiOjE3NzQ4Njg5NzEsImV4cCI6MTc3NDk1NTYxMSwiYWNyIjoiMSIsImFpbyI6IkFVUUF1LzhiQUFBQUJLYVRLVWNPR2o3VExYODdoN2xLRC8vNTVUNXU3V0FYU2duZjN2UUJCMVh1RlhqQk1GbjBNREdERGRXckJGNjdDZ1lJektZcWNsdjJicEE4VEdpMkd3PT0iLCJhbXIiOlsicHdkIl0sImFwcGlkIjoiNzNiYzE3ODctYWY4ZS00NDBjLWE1MGEtY2ViMjkwMzBhNjNjIiwiYXBwaWRhY3IiOiIwIiwiZmFtaWx5X25hbWUiOiJCQVJSRVRPIiwiZ2l2ZW5fbmFtZSI6Ik1BUklBTkEiLCJpcGFkZHIiOiIyMDAuMjQzLjUzLjIiLCJuYW1lIjoiTUFSSUFOQSBWSUVJUkEgQkFSUkVUTyIsIm9pZCI6ImJiNDFiYjI4LTYyZTAtNDJmYS1hYTZmLTgxNTc0ZjdmYmIzZCIsIm9ucHJlbV9zaWQiOiJTLTEtNS0yMS0zMzc1NzU0OTIzLTM0NTg0NDM2NTgtNjc0OTc2ODE0LTQ1ODgxMCIsInJoIjoiMS5BVmtBUktoSjJ1UGlyMENHcHNPQm5YQlBTWWNYdkhPT3J3eEVwUXJPc3BBd3BqeFpBQWhaQUEuIiwic2NwIjoiVXNlci5SZWFkIiwic2lkIjoiMDAzMTc4MmEtNjA1ZC05ZjI5LThmNTQtMmZhYjhmMjYyMmJmIiwic3ViIjoiZmh3WlpMSGQ0XzhfMTZYRU05eDVYVk5PQWlUMTJSSFNGZnItNEVpMDdfQSIsInRpZCI6ImRhNDlhODQ0LWUyZTMtNDBhZi04NmE2LWMzODE5ZDcwNGY0OSIsInVuaXF1ZV9uYW1lIjoiMjAyNDA3MDk2MzAxQGFsdW5vcy5pYm1lYy5lZHUuYnIiLCJ1cG4iOiIyMDI0MDcwOTYzMDFAYWx1bm9zLmlibWVjLmVkdS5iciIsInV0aSI6IkRuSGw3MElVTTAtYU5Tb2hxek5OQUEiLCJ2ZXIiOiIxLjAiLCJ4bXNfZnRkIjoiOWFkMUdlOXg3dUQxWGxaWXBjRGxKdFUyY0ZueDlxRGpjbWR0UlVkMUZYOEJkWE4zWlhOME15MWtjMjF6In0.N_UBjpvtctrAbIenvhMev8hlIXgnYeX8WC0rt4pEWNmyhW5kOZvfTuCG_D1rShugpIWjQC7bSx_Kd_34f6zwGYqv3UEfEkekgcHsYN_x53nWVA3XOBI8IlPtFFoK0mVccL75aaMeAH6dO7qWDTjdOtvcEY-8c_ekDGvUD50n0F0wPI0WaoBhlsaLEXGsEHTsGRIBKx-GHWP4n14gdsylGh1I6a5-FNJxi3R7HuPrZvdbKVNN7734XOIdPW6bKbzUkTdGY6ffpENLCH-JOGGT7TZNlhWbueLkXJGCRMlYFfNAVZaPKz88F_XQOP4tZILbwaLBkNswKq6O5SjLPHpFTA"

headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://estudante.ibmec.br/",
    "Origin": "https://estudante.ibmec.br"
}

turmas_ids = [
    "ibmec_14288731",
    "ibmec_14288775",
    "ibmec_14288583",
    "ibmec_14288612",
    "ibmec_14288690"
]

dados = []

for turma_id in turmas_ids:

    # 🔹 1. pegar nome da disciplina
    url_turma = f"https://apis.estudante.ibmec.br/rest/turmas/{turma_id}?matricula=202501007139"
    r_turma = requests.get(url_turma, headers=headers)

    if r_turma.status_code != 200:
        print(f"Erro turma {turma_id}")
        continue

    data_turma = r_turma.json()
    nome_disciplina = data_turma["disciplina"]["nome"]

    # 🔹 2. pegar conteúdos complementares
    url_conteudos = f"https://apis.estudante.ibmec.br/rest/v2/entregas/{turma_id}/conteudos-complementares"
    r_cont = requests.get(url_conteudos, headers=headers)

    print(f"Turma: {turma_id} | Status: {r_cont.status_code}")

    if r_cont.status_code == 200:
        conteudos = r_cont.json()

        for item in conteudos:
            dados.append({
                "turma_id": turma_id,
                "disciplina": nome_disciplina,
                "titulo": item.get("titulo"),
                "descricao": item.get("descricao"),
                "tipo": item.get("tipo"),
                "url": item.get("url"),
                "tags": ", ".join(item.get("tags", []))
            })

# dataframe final
df = pd.DataFrame(dados)

print(df)

df.to_csv("conteudos_completos.csv", index=False)

print("\n✅ CSV com disciplina + conteúdos gerado!")