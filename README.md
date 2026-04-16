# 📊 Análise de Dados em Python

Este projeto tem como objetivo realizar o processamento, análise e geração de métricas a partir de arquivos CSV, utilizando Python e bibliotecas voltadas para manipulação de dados e paralelismo.

---

## 🚀 Funcionalidades

* Leitura de múltiplos arquivos CSV
* Consolidação de dados
* Agrupamentos e cálculos estatísticos
* Processamento paralelo para melhor desempenho
* Geração de relatórios por tribunal

---

## 🛠️ Tecnologias utilizadas

* **Python 3**
* **pandas** – manipulação e análise de dados
* **numpy** – operações numéricas
* **pathlib** – manipulação de caminhos de arquivos
* **os / glob** – gerenciamento de arquivos
* **time** – medição de desempenho
* **multiprocessing / concurrent.futures** – paralelismo

---

## 📁 Estrutura do projeto

```
Analide_de_dados_em_Python/
│
├── Base de Dados/          # Arquivos CSV utilizados
├── arquivos_concatenados.csv
├── main.py                 # Arquivo principal
├── ...                     # Outros módulos
```

---

## ⚙️ Como executar o projeto

1. Clone o repositório:

```bash
git clone https://github.com/LuizHerran/Analide_de_dados_em_Python.git
```

2. Acesse a pasta do projeto:

```bash
cd Analide_de_dados_em_Python
```

3. Instale as dependências (se necessário):

```bash
pip install pandas numpy
```

4. Execute o programa:

```bash
python main.py
```

---

## 📌 Como funciona

O sistema percorre os arquivos CSV presentes na pasta **Base de Dados**, realiza o processamento dos dados e aplica operações como:

* Agrupamento por tribunal (`groupby`)
* Soma de colunas numéricas
* Cálculo de métricas (ex: metas e indicadores)

Também é possível utilizar **processamento paralelo**, dividindo os dados em partes menores para melhorar o desempenho em grandes volumes de dados.

---

## ⚡ Paralelismo

O projeto utiliza abordagens como:

* `multiprocessing`
* `ThreadPoolExecutor`

Isso permite que múltiplos arquivos sejam processados simultaneamente, reduzindo o tempo total de execução.

---

## 🧠 Conceitos aplicados

* Manipulação de dados com DataFrames
* Processamento paralelo
* Organização modular de código
* Leitura eficiente de arquivos

---

## 📈 Possíveis melhorias

* Interface gráfica ou web
* Exportação de relatórios em Excel ou PDF
* Testes automatizados
* Logs estruturados

---

## 👤 Autor

Desenvolvido por **Luiz Rocha**, **Victor Nogueira**, **Matheus Rios** e **Gustavo Xavier**

---

## 📄 Licença

Este projeto é de uso acadêmico e educacional.
