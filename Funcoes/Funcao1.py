#Primerio abrir e concatenar todos arquvios em 1
import os
from pathlib import Path

def concatenar_arquivos():
    x = 1
    #O arquivo "Base de Dados" deve estár na mesma apsta da main
    base_dir = Path(__file__).parent
    pasta = base_dir / 'Base de Dados'
    print("Concatendando:")
    #Percorre por todos arquivos da pasta que sejam .csv
    for arquivo in pasta.glob("*.csv"):

        #Armazena o caminho de cada arquivo
        caminho_completo = os.path.join(pasta, arquivo)

        #Abre le os arquivos pelo seu enderço e o padroniza no formato "utf-8"
        with open(caminho_completo, "r", encoding="utf-8") as f:

            #Le todo o arquivo e armazena linha por linha dentro de conteudo como uma string
            conteudo = f.read()

            #Guarda cada arquivo lido em um novo arquivo assim concatenando todos arquivos em 1
            with open("arquivos_concatenados.csv", "a", encoding="utf-8") as f:
                f.write(conteudo)
                print(f"Arquivo: {x}")
                x = x+1
    print("Arquivos concatenados!")
