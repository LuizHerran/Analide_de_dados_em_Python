import time
import csv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# FUNÇÃO AUXILIAR
#Criei essa função para não ter que digitar esse dicionário gigante toda hora
#ela serve para zerar os contadores de um município novo que aparecer
def inicializar_municipio():
    return {
        "julgados_2026": 0,
        "casos_novos_2026": 0,
        "dessobrestados_2026": 0,
        "suspensos_2026": 0,
        "julgm2_a": 0,
        "distm2_a": 0,
        "suspm2_a": 0,
        "julgm2_ant": 0,
        "distm2_ant": 0,
        "suspm2_ant": 0,
        "desom2_ant": 0,
        "julgm4_a": 0,
        "distm4_a": 0,
        "suspm4_a": 0,
        "julgm4_b": 0,
        "distm4_b": 0,
        "suspm4_b": 0
    }

# SERIAL
# Essa é a versão que processa um arquivo por vez, do jeito padrão
def processar_serial():
    dados = {} # Dicionário mestre para guardar tudo

    base_dir = Path(__file__).parent
    pasta = base_dir / 'Base de Dados'
    
    for arquivo_csv in pasta.glob("*.csv"):
        #Abro o arquivo para leitura
        with open(arquivo_csv, "r", encoding="utf-8") as arquivo:
            leitor = csv.DictReader(arquivo)#o DictReader para usar os nomes das colunas
            for linha in leitor:
                municipio = linha["municipio_oj"] #o nome da cidade
                # Se a cidade ainda não tá no meu dicionário, eu crio ela zerada
                if municipio not in dados:
                    dados[municipio] = inicializar_municipio()
                #o try para o código não travar se tiver algum dado vazio ou errado no CSV
                try:
                    d = dados[municipio]
                    # Vou somando cada valor nas suas respectivas chaves (convertendo para float)
                    d["julgados_2026"] += float(linha["julgados_2026"])
                    d["casos_novos_2026"] += float(linha["casos_novos_2026"])
                    d["dessobrestados_2026"] += float(linha["dessobrestados_2026"])
                    d["suspensos_2026"] += float(linha["suspensos_2026"])
                    d["julgm2_a"] += float(linha["julgm2_a"])
                    d["distm2_a"] += float(linha["distm2_a"])
                    d["suspm2_a"] += float(linha["suspm2_a"])
                    d["julgm2_ant"] += float(linha["julgm2_ant"])
                    d["distm2_ant"] += float(linha["distm2_ant"])
                    d["suspm2_ant"] += float(linha["suspm2_ant"])
                    d["desom2_ant"] += float(linha["desom2_ant"])
                    d["julgm4_a"] += float(linha["julgm4_a"])
                    d["distm4_a"] += float(linha["distm4_a"])
                    d["suspm4_a"] += float(linha["suspm4_a"])
                    d["julgm4_b"] += float(linha["julgm4_b"])
                    d["distm4_b"] += float(linha["distm4_b"])
                    d["suspm4_b"] += float(linha["suspm4_b"])

                except:
                    pass# Se der erro em uma linha, pula para a próxima
    return dados

# PARALELO
# Essa função faz a mesma coisa que a serial, mas foca em um único arquivo
def processar_arquivo(caminho):
    dados_local = {}
    with open(caminho, "r", encoding="utf-8") as arquivo:
        leitor = csv.DictReader(arquivo)

        for linha in leitor:
            municipio = linha["municipio_oj"]
            if municipio not in dados_local:
                dados_local[municipio] = inicializar_municipio()

            try:
                d = dados_local[municipio]
                # Repetindo a lógica de somar os valores
                d["julgados_2026"] += float(linha["julgados_2026"])
                d["casos_novos_2026"] += float(linha["casos_novos_2026"])
                d["dessobrestados_2026"] += float(linha["dessobrestados_2026"])
                d["suspensos_2026"] += float(linha["suspensos_2026"])
                d["julgm2_a"] += float(linha["julgm2_a"])
                d["distm2_a"] += float(linha["distm2_a"])
                d["suspm2_a"] += float(linha["suspm2_a"])
                d["julgm2_ant"] += float(linha["julgm2_ant"])
                d["distm2_ant"] += float(linha["distm2_ant"])
                d["suspm2_ant"] += float(linha["suspm2_ant"])
                d["desom2_ant"] += float(linha["desom2_ant"])
                d["julgm4_a"] += float(linha["julgm4_a"])
                d["distm4_a"] += float(linha["distm4_a"])
                d["suspm4_a"] += float(linha["suspm4_a"])
                d["julgm4_b"] += float(linha["julgm4_b"])
                d["distm4_b"] += float(linha["distm4_b"])
                d["suspm4_b"] += float(linha["suspm4_b"])

            except:
                pass
    return dados_local

#  multi-thread
def processar_paralelo():

    base_dir = Path(__file__).parent
    pasta = base_dir / 'Base de Dados'

    # lista com o caminho de todos os CSVs da pasta
    arquivos = [ arquivo for arquivo in pasta.glob("*.csv") ]
    resultado_final = {}

    #o ThreadPool para mandar vários arquivos serem processados ao mesmo tempo
    with ThreadPoolExecutor() as executor:
        resultados = executor.map(processar_arquivo, arquivos)

    # Depois que os arquivos terminam,preciso juntar os resultados de cada thread
    for resultado in resultados:
        for municipio, dados in resultado.items():
            if municipio not in resultado_final:
                resultado_final[municipio] = inicializar_municipio()
            # Somo os totais parciais no meu dicionário principal
            for chave in dados:
                resultado_final[municipio][chave] += dados[chave]

    return resultado_final

# Função para calcular as metas e gerar o arquivo de saída
def salvar_resumo(dados):
    # Função interna para fazer a divisão e multiplicar por 100 (evita erro de divisão por zero)
    def calc(dividendo, divisor):
        return (dividendo / divisor) * 100 if divisor != 0 else 0

    # Abro o arquivo novo que vai ser o meu relatório
    with open("resumo.csv", "w", newline="", encoding="utf-8") as saida:
        campos = ["municipio", "julgados_2026", "Meta1", "Meta2A", "Meta2Ant", "Meta4A", "Meta4B"]
        escritor = csv.DictWriter(saida, fieldnames=campos)
        escritor.writeheader() #Escreve os títulos das colunas

        #Para cada município, calculo as metas 
        for municipio, d in dados.items():
            # Meta 1: Julgados / (Casos Novos + Dessobrestados - Suspensos) * 100

            meta1 = calc(
                d["julgados_2026"],
                d["casos_novos_2026"] + d["dessobrestados_2026"] - d["suspensos_2026"]
            )

            # Meta 2A: Aplica o fator 1000 e divide por 7 (ajuste da fórmula)
            meta2a = calc(
                d["julgm2_a"],
                d["distm2_a"] - d["suspm2_a"]
            )* 10 / 7 
            # Meta 2Ant: Julgados / (Distribuídos - Suspensos - Desom) * 100
            meta2ant = calc(
                d["julgm2_ant"],
                d["distm2_ant"] - d["suspm2_ant"] - d["desom2_ant"]
            )

            meta4a = calc(
                d["julgm4_a"],
                d["distm4_a"] - d["suspm4_a"]
            )

            meta4b = calc(
                d["julgm4_b"],
                d["distm4_b"] - d["suspm4_b"]
            )
            # Salavaa linha no CSV arredondando tudo para 2 casas decimais
            escritor.writerow({
                "municipio": municipio,
                "julgados_2026": round(d["julgados_2026"], 2),
                "Meta1": round(meta1, 2),
                "Meta2A": round(meta2a, 2),
                "Meta2Ant": round(meta2ant, 2),
                "Meta4A": round(meta4a, 2),
                "Meta4B": round(meta4b, 2)
            })

#Parte visual para o usuário escolher o que ele quer fazer
def menu():
    while True:
        print("\n**** SISTEMA CSV *******")
        print("1 -Rodar versão SERIAL")
        print("2 -Rodar versão PARALELA")
        print("3 -Comparar desempenho")
        print("4 -Sair")
        opcao = input("Escolha: ")

        if opcao == "1":

            inicio = time.time() #Começa a contar o tempo
            dados = processar_serial()
            fim = time.time() #Para de contar
            salvar_resumo(dados)
            print(f"Tempo SERIAL: {fim - inicio:.4f} segundos")

        elif opcao == "2":

            inicio = time.time()
            dados = processar_paralelo()
            fim = time.time()
            salvar_resumo(dados)
            print(f"Tempo PARALELO: {fim - inicio:.4f} segundos")
        elif opcao == "3":

            #roda as duas versões para ver qual é mais rápida
            inicio = time.time()
            processar_serial()
            tempo_serial = time.time() - inicio
            inicio = time.time()
            processar_paralelo()
            tempo_paralelo = time.time() - inicio

            #calculaa quantas vezes a parallela é mais rápida que a serial
            speedup = tempo_serial / tempo_paralelo if tempo_paralelo != 0 else 0
            print(f"\nTempo Serial: {tempo_serial:.4f}s")
            print(f"Tempo Paralelo: {tempo_paralelo:.4f}s")
            print(f"Speedup: {speedup:.2f}x")
            if (speedup):
                print("Analise de Sucesso!")
            else:
                print("Analise de Falha de Desempenho!")
        elif opcao == "4":
            print("Saindo...")
            break
        else:
            print("Opção inválida!")