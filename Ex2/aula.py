import re

frase = "Meu CPF é 123.456.789-10"

numeros = re.findall(r"\d", frase)

print(numeros)