import time
import os
import csv
from pathlib import Path
from multiprocessing import Pool, cpu_count

# Função auxiliar (executada em paralelo)
def processar_arquivo(args):
    file, municipio = args
    rows = []

    with open(file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('municipio_oj', '').upper() == municipio:
                rows.append(row)
    return rows


def relatorio_de_municipio_paralelo(municipio):
    base_dir = Path(__file__).parent
    files = list((base_dir / 'Base de Dados').glob('*.csv'))

    print("Processando em paralelo...")

    # Prepara os argumentos (arquivo + município)
    args = [(file, municipio) for file in files]

    # Cria pool de processos
    with Pool(cpu_count()) as pool:
        resultados = pool.map(processar_arquivo, args)

    # Junta todas as linhas retornadas
    rows = []
    for resultado in resultados:
        rows.extend(resultado)

    output_file = f'{municipio}.csv'

    # Escreve o resultado final
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    count = len(rows)
    print(f'Arquivo {municipio}.csv criado em paralelo com sucesso! ({count} ocorrências)')

#Seriel
def relatorio_de_municipio_serial(municipio):
    # Lista todos os arquivos CSV na pasta 'Base de Dados'
    base_dir = Path(__file__).parent
    files = base_dir / 'Base de Dados'

    # Variável para armazenar as linhas filtradas
    rows = []

    # Lê cada CSV e filtra as linhas do município
    for file in files.glob('*.csv'):
        with open(os.path.join('./Base de Dados', file), 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('municipio_oj', '').upper() == municipio:
                    rows.append(row)

    # Nome do arquivo de saída
    output_file = f'{municipio}.csv'

    # Salva as ocorrências em um novo arquivo CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    count = len(rows)
    print(f'Arquivo {municipio}.csv criado em serial com sucesso! ({count} ocorrências)')

def relatorio_de_municipio():
    municipio = input('Digite o município: ').strip().upper()
    iniciop = time.time()
    relatorio_de_municipio_paralelo(municipio)
    fimp = time.time()
    tparalelo = fimp - iniciop

    inicios = time.time()
    relatorio_de_municipio_serial(municipio)
    fims = time.time()
    tserial = fims - inicios
    print(f"Concatenação em paralelo: {tparalelo:.4f} segundos")
    print(f"Concatenação em Serial: {tserial:.4f} segundos")

    print(f"Speedup: {tserial / tparalelo:.2f}x")
    if (tserial / tparalelo > 1):
        print("Analise de Sucesso!")
    else:
        print("Analise de Falha de Desempenho!")