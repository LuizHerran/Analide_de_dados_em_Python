#Primerio abrir e concatenar todos arquvios em 1
import os
import time
from pathlib import Path
from multiprocessing import Pool, cpu_count

#De forma Paralela
def ler_arquivo(caminho):
    with open(caminho, "r", encoding="utf-8") as f:
        return f.read()

def concatenar_arquivos_paralelo():
    base_dir = Path(__file__).parent
    pasta = base_dir / 'Base de Dados'

    arquivos = list(pasta.glob("*.csv"))

    print("Concatenando em paralelo...")

    # Cria um pool de processos e usa um nucleo para cada processo (cada nucleo lé um arquivo ao mesmo tempo)
    with Pool(cpu_count()) as pool:
        conteudos = pool.map(ler_arquivo, arquivos)

    # Escrita final (única, segura)
    with open("arquivos_concatenados.csv", "w", encoding="utf-8") as f:
        for i, conteudo in enumerate(conteudos, start=1):
            f.write(conteudo)
            print(f"Arquivo: {i}")
    print("Arquivos concatenados!")

#De forma Serial
def concatenar_arquivos_serial():
    x = 1
    #O arquivo "Base de Dados" deve estár na mesma apsta da main
    base_dir = Path(__file__).parent
    pasta = base_dir / 'Base de Dados'
    print("Concatendando em Serial:")
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

def concatenacao():
    iniciop = time.time()
    concatenar_arquivos_paralelo()
    fimp = time.time()
    tparalelo = fimp - iniciop

    inicios = time.time()
    concatenar_arquivos_serial()
    fims = time.time()
    tserial = fims - inicios
    print(f"Concatenação em paralelo: {tparalelo:.4f} segundos")
    print(f"Concatenação em Serial: {tserial:.4f} segundos")

    print(f"Speedup: {tserial/tparalelo:.2f}x")
    if(tserial/tparalelo > 1):
        print("Analise de Sucesso!")
    else:
        print("Analise de Falha de Desempenho!")