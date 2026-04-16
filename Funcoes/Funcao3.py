# Importa o módulo os para interações com o sistema de arquivos
import os
# Importa a biblioteca pandas para manipulação de dados em DataFrames
import pandas as pd
# Importa a classe Path para trabalhar com caminhos de forma mais moderna e segura
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import numpy as np
import time

#Função auxiliar que processa uma parte do DataFrame.
def processar_chunk(chunk, colunas_numericas):
    # Limpeza de colunas e conversão numérica
    chunk.columns = chunk.columns.str.strip()
    for col in colunas_numericas:
        if col in chunk.columns:
            chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

    # Agrupamento parcial para reduzir o tamanho dos dados que retornam ao processo principal
    return chunk.groupby('sigla_tribunal').sum(numeric_only=True)

def arquivo_resumo_meta_tribunal_paralelo():
    base_dir = Path(__file__).parent.parent
    caminho = base_dir / 'arquivos_concatenados.csv'

    if not os.path.exists(caminho):
        print("Arquivo não encontrado.")
        return

    colunas_numericas = [
        'julgados_2026', 'casos_novos_2026', 'dessobrestados_2026', 'suspensos_2026',
        'julgm2_a', 'distm2_a', 'suspm2_a',
        'julgm2_ant', 'distm2_ant', 'suspm2_ant', 'desom2_ant',
        'julgm4_a', 'distm4_a', 'suspm4_a',
        'julgm4_b', 'distm4_b', 'suspm4_b'
    ]

    #Isso evita estourar a memória e permite distribuir o trabalho
    chunk_size = 50000  # Ajuste conforme o tamanho do seu arquivo
    chunks = pd.read_csv(caminho, sep=',', low_memory=False, chunksize=chunk_size)

    resultados_parciais = []

    #Usamos o ProcessPoolExecutor para CPU-bound tasks
    with ProcessPoolExecutor() as executor:
        #Mapeia a função de processamento para cada pedaço do arquivo
        futures = [executor.submit(processar_chunk, chunk, colunas_numericas) for chunk in chunks]

        for future in futures:
            resultados_parciais.append(future.result())

    #CONSOLIDA RESULTADOS - Soma os resultados de todos os processos
    Organizador_siglas = pd.concat(resultados_parciais).groupby(level=0).sum()

    def calcular_metas(df):
        df['Meta1'] = (df['julgados_2026'] / (
                    df['casos_novos_2026'] + df['dessobrestados_2026'] + df['suspensos_2026'])) * 100
        df['Meta2A'] = (df['julgm2_a'] / (df['distm2_a'] + df['suspm2_a'])) * 100
        df['Meta2Ant'] = (df['julgm2_ant'] / (df['distm2_ant'] + df['suspm2_ant'] + df['desom2_ant'])) * 100
        df['Meta4A'] = (df['julgm4_a'] / (df['distm4_a'] + df['suspm4_a'])) * 100
        df['Meta4B'] = (df['julgm4_b'] / (df['distm4_b'] + df['suspm4_b'])) * 100
        return df

    Organizador_siglas = calcular_metas(Organizador_siglas)

    #Limpa infinitos e formata
    Organizador_siglas.replace([np.inf, -np.inf], 0, inplace=True)
    Organizador_siglas = Organizador_siglas.reset_index().sort_values(by='Meta1', ascending=False)

    top10 = Organizador_siglas.head(10)[['sigla_tribunal', 'Meta1', 'Meta2A', 'Meta2Ant', 'Meta4A', 'Meta4B']]

    top10_formatado = top10.copy()
    metas_cols = ['Meta1', 'Meta2A', 'Meta2Ant', 'Meta4A', 'Meta4B']
    for col in metas_cols:
        top10_formatado[col] = top10_formatado[col].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else "N/A")

    print("\n=== TOP 10 TRIBUNAIS (PARALELO) ===")
    print(top10_formatado.to_string(index=False))
    top10.to_csv('top10_tribunais.csv', index=False)

# Define a função principal que gera o resumo por tribunal
def arquivo_resumo_meta_tribunal_serial():

    # Define o diretório base como o diretório onde o script está localizado
    base_dir = Path(__file__).parent.parent

    # Define o caminho completo para o arquivo CSV concatenado
    caminho = base_dir / 'arquivos_concatenados.csv'

    # Verifica se o arquivo existe no caminho especificado
    if not os.path.exists(caminho):
        # Exibe mensagem de erro caso o arquivo não exista
        print("O arquivo 'arquivos_concatenados.csv' não foi encontrado. Certifique-se de executar a função de concatenação primeiro.")
        # Encerra a execução da função
        return

    # Carrega o arquivo CSV em um DataFrame do pandas
    df = pd.read_csv(caminho, sep=',', low_memory=False)

    df.columns = df.columns.str.strip()
    #print("Colunas encontradas:", df.columns.tolist()) # remover depois

   # GARANTE QUE COLUNAS NUMÉRICAS SÃO NUMÉRICAS
    colunas_numericas = [
        'julgados_2026', 'casos_novos_2026', 'dessobrestados_2026', 'suspensos_2026',
        'julgm2_a', 'distm2_a', 'suspm2_a',
        'julgm2_ant', 'distm2_ant', 'suspm2_ant', 'desom2_ant',
        'julgm4_a', 'distm4_a', 'suspm4_a',
        'julgm4_b', 'distm4_b', 'suspm4_b'
    ]

    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            print(f"Coluna ausente: {col}")

    # GROUPBY
    Organizador_siglas = df.groupby('sigla_tribunal').sum(numeric_only=True)

    # CÁLCULOS (com proteção contra divisão por zero)
    Organizador_siglas['Meta1'] = (
        Organizador_siglas['julgados_2026'] /
        (Organizador_siglas['casos_novos_2026'] +
         Organizador_siglas['dessobrestados_2026'] +
         Organizador_siglas['suspensos_2026'])
    ) * 100

    Organizador_siglas['Meta2A'] = (
        Organizador_siglas['julgm2_a'] /
        (Organizador_siglas['distm2_a'] +
         Organizador_siglas['suspm2_a'])
    ) * 100

    Organizador_siglas['Meta2Ant'] = (
        Organizador_siglas['julgm2_ant'] /
        (Organizador_siglas['distm2_ant'] +
         Organizador_siglas['suspm2_ant'] +
         Organizador_siglas['desom2_ant'])
    ) * 100

    Organizador_siglas['Meta4A'] = (
        Organizador_siglas['julgm4_a'] /
        (Organizador_siglas['distm4_a'] +
         Organizador_siglas['suspm4_a'])
    ) * 100

    Organizador_siglas['Meta4B'] = (
        Organizador_siglas['julgm4_b'] /
        (Organizador_siglas['distm4_b'] +
         Organizador_siglas['suspm4_b'])
    ) * 100

    # LIMPA VALORES INFINITOS (divisão por zero)
    Organizador_siglas.replace([float('inf'), -float('inf')], 0, inplace=True)

    Organizador_siglas = Organizador_siglas.reset_index()
    Organizador_siglas = Organizador_siglas.sort_values(by='Meta1', ascending=False)

    top10 = Organizador_siglas.head(10)

    top10 = top10[['sigla_tribunal', 'Meta1', 'Meta2A', 'Meta2Ant', 'Meta4A', 'Meta4B']]

    top10_formatado = top10.copy()

    # esse loop formata as colunas de metas para exibir como porcentagem com 2 casas decimais, ou "N/A" se o valor for nulo
    for col in ['Meta1', 'Meta2A', 'Meta2Ant', 'Meta4A', 'Meta4B']:
        top10_formatado[col] = top10_formatado[col].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else "N/A")

    print("\n=== TOP 10 TRIBUNAIS ===")
    print(top10_formatado.to_string(index=False))

    top10.to_csv('top10_tribunais.csv', index=False)

    print("Top 10 gerado com sucesso!")

def arquivo_resumo_meta_tribunal():
    iniciop = time.time()
    arquivo_resumo_meta_tribunal_paralelo()
    fimp = time.time()
    tparalelo = fimp - iniciop

    inicios = time.time()
    arquivo_resumo_meta_tribunal_serial()
    fims = time.time()
    tserial = fims - inicios
    print(f"Concatenação em paralelo: {tparalelo:.4f} segundos")
    print(f"Concatenação em Serial: {tserial:.4f} segundos")

    print(f"Speedup: {tserial / tparalelo:.2f}x")
    if (tserial / tparalelo > 1):
        print("Analise de Sucesso!")
    else:
        print("Analise de Falha de Desempenho!")