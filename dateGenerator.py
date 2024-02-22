# Script gerado pelo ChatGPT-3.5

from datetime import datetime, timedelta


def gerar_sequencia_de_datas(data_inicial, data_final):
    datas = []
    data_atual = data_inicial
    while data_atual <= data_final:
        datas.append(data_atual.strftime("%d-%m-%Y"))
        data_atual += timedelta(days=1)
    return datas


# Data inicial e final da sequência
data_inicial = datetime(2022, 3, 5)
data_final = datetime(2024, 2, 18)

# Gerar a sequência de datas
sequencia_datas = gerar_sequencia_de_datas(data_inicial, data_final)

# Salvar as datas em um arquivo txt
nome_arquivo = "build/dateSequence.txt"
with open(nome_arquivo, "w", encoding='utf-8') as arquivo:
    for data in sequencia_datas:
        arquivo.write(f'{data}\n')
