# Importa o módulo os para interações com o sistema de arquivos
import os

# Importa a biblioteca pandas para manipulação de dados em DataFrames
import pandas as pd

# Importa a classe Path para trabalhar com caminhos de forma mais moderna e segura
from pathlib import Path

# Define a função principal que gera o resumo por tribunal
def arquivo_resumo_meta_tribunal():

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
    df = pd.read_csv(caminho)

    # Agrupa os dados pela coluna 'sigla_tribunal' somando apenas colunas numéricas
    Organizador_siglas = df.groupby('sigla_tribunal').sum(numeric_only=True)

    # Calcula a Meta1:
    # (processos julgados em 2026) dividido pela soma de:
    # casos novos + dessobrestados + suspensos, multiplicado por 100 (percentual)
    Organizador_siglas['Meta1'] = (
        Organizador_siglas['julgados_2026'] /
        (Organizador_siglas['casos_novos_2026'] +
         Organizador_siglas['dessobrestados_2026'] +
         Organizador_siglas['suspensos_2026'])
    ) * 100

    # Calcula a Meta2A:
    # julgados meta 2 (ano atual) dividido por distribuídos + suspensos
    Organizador_siglas['Meta2A'] = (
        Organizador_siglas['julgm2_a'] /
        (Organizador_siglas['distm2_a'] +
         Organizador_siglas['suspm2_a'])
    ) * 100

    # Calcula a Meta2Ant (anos anteriores):
    # julgados dividido por distribuídos + suspensos + desconsiderados
    Organizador_siglas['Meta2Ant'] = (
        Organizador_siglas['julgm2_ant'] /
        (Organizador_siglas['distm2_ant'] +
         Organizador_siglas['suspm2_ant'] +
         Organizador_siglas['desom2_ant'])
    ) * 100

    # Calcula a Meta4A:
    # julgados dividido por distribuídos + suspensos
    Organizador_siglas['Meta4A'] = (
        Organizador_siglas['julgm4_a'] /
        (Organizador_siglas['distm4_a'] +
         Organizador_siglas['suspm4_a'])
    ) * 100

    # Calcula a Meta4B:
    # julgados dividido por distribuídos + suspensos
    Organizador_siglas['Meta4B'] = (
        Organizador_siglas['julgm4_b'] /
        (Organizador_siglas['distm4_b'] +
         Organizador_siglas['suspm4_b'])
    ) * 100

    # Reseta o índice para transformar 'sigla_tribunal' de índice para coluna normal
    Organizador_siglas = Organizador_siglas.reset_index()

    # Ordena os tribunais pela Meta1 em ordem decrescente (maiores valores primeiro)
    Organizador_siglas = Organizador_siglas.sort_values(by='Meta1', ascending=False)

    # Seleciona os 10 primeiros tribunais (top 10)
    top10 = Organizador_siglas.head(10)

    # Mantém apenas as colunas relevantes para o resultado final
    top10 = top10[['sigla_tribunal', 'Meta1', 'Meta2A', 'Meta2Ant', 'Meta4A', 'Meta4B']]

    # Salva o resultado em um novo arquivo CSV sem incluir o índice
    top10.to_csv('top10_tribunais.csv', index=False)

    # Exibe mensagem de sucesso
    print("Top 10 gerado com sucesso!")
