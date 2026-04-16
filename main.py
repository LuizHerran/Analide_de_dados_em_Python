import sys
from Funcoes.Funcao1 import concatenar_arquivos
from Funcoes.Funcao3 import arquivo_resumo_meta_tribunal
from Funcoes.Funcao2 import menu
from Funcoes.Funcao4 import relatorio_de_muncipio

def menu_opcoes():
    print("""//=======================================================\\\\
||  
||                  MENU
|| 1 - Concatenar todos dados e gerar um novo arquivo.
|| 2 - Resumir meta tributal
|| 3 - Gerar resumo das medidas dos tribunais.
|| 4 - Relatorio de Municipio espeficio.
||
|| (0) - Sair.
\\\\=======================================================//""")
    opc = int(input('Digite a opção: '))

    sair = False

    while sair == False:
        match opc:
            case 1:
                concatenar_arquivos()
                menu_opcoes()
            case 2:
                menu()
                menu_opcoes()
            case 3:
                arquivo_resumo_meta_tribunal()         #Em correção!!
                menu_opcoes()
            case 4:
                relatorio_de_muncipio()
                menu_opcoes()
            case 0:
                sys.exit()
            case _:
                print("||\n|| Escolha uma opção valida!\n||")
                opc = int(input('|| Digite a opção: '))
                continue

menu_opcoes()