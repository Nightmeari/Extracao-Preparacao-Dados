import re

frases = [
    "Meu CPF é 123.456.789-10",
    "Cadastrar CPF 987.654.321-00",
    "CPFs do cliente: 111.222.333-44 e 059.362.136-09"
]

for frase in frases:
    cpfs = re.findall(r"\d{3}\.\d{3}\.\d{3}-\d{2}", frase)
    print(cpfs)