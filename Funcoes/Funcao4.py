import csv
import os
import csv
from pathlib import Path

def relatorio_de_muncipio():
    # Lista todos os arquivos CSV na pasta 'Base de Dados'
    base_dir = Path(__file__).parent
    files = base_dir / 'Base de Dados'

    # Solicita o município ao usuário
    municipio = input('Digite o município: ').strip().upper()

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
    print(f'Arquivo {municipio}.csv criado com sucesso! ({count} ocorrências)')