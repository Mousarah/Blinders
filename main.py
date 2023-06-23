from updateDB import UpdateDB
import sqlite3
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta


def get_escolha_moeda():
    try:
        resposta = int(input("-> "))

        if resposta in (1, 2, 3):
            if resposta == 1:
                return "bitcoin"
            elif resposta == 2:
                return "ethereum"
            else:
                return "polygon"
        else:
            raise ValueError()

    except ValueError as e:
        print("#ERRO: OPÇÃO INVÁLIDA!")
        return get_escolha_moeda()


def rsi(dados, periodo):
    dados['change'] = dados['FECHAMENTO'] - dados['FECHAMENTO'].shift(1)

    dados['gain'] = dados.loc[dados['change'] > 0, 'change'].apply(abs)
    dados.loc[(dados['gain'].isna()), 'gain'] = 0
    dados[0, dados.columns.get_loc('gain')] = np.NaN

    dados['loss'] = dados.loc[dados['change'] < 0, 'change'].apply(abs)
    dados.loc[(dados['loss'].isna()), 'loss'] = 0
    dados[0, dados.columns.get_loc('loss')] = np.NaN

    dados['avg_gain'] = dados['gain'].rolling(periodo).mean()
    dados['avg_loss'] = dados['loss'].rolling(periodo).mean()

    first = dados['avg_gain'].first_valid_index()

    prev_avg_gain = 0
    prev_avg_loss = 0
    for index, row in dados.iterrows():
        if index == first:
            prev_avg_gain = row['avg_gain']
            prev_avg_loss = row['avg_loss']
        elif index > first:
            dados.loc[index, 'avg_gain'] = ((prev_avg_gain * (periodo - 1)) + row['gain']) / periodo
            prev_avg_gain = dados.loc[index, 'avg_gain']

            dados.loc[index, 'avg_loss'] = ((prev_avg_loss * (periodo - 1)) + row['loss']) / periodo
            prev_avg_loss = dados.loc[index, 'avg_loss']

    dados[f'RS{periodo}'] = dados['avg_gain'] / dados['avg_loss']
    dados[f'RSI{periodo}'] = 100 - (100 / (1 + dados[f'RS{periodo}']))

    lista = dados.columns.to_list()
    matching = [s for s in lista if "RSI" in s]
    sel_col = ['FECHAMENTO'] + matching

    return dados[sel_col].copy()


if __name__ == "__main__":
    # ATUALIZAÇÃO DA BASE DE DADOS
    UpdateDB()

    # MENU DE ESCOLHA DA MOEDA A SER ANALISADA
    print("#" * 32)
    print("Escolha a moeda a ser analisada:")
    print("1 - Bitcoin")
    print("2 - Ethereum")
    print("3 - Polygon")
    print("#" * 32)

    tabela = get_escolha_moeda()
    print("#INFO: VOCÊ ESCOLHEU " + tabela.upper())
    time.sleep(1)

    # MENU DE ESCOLHA DO PERIODO DE EXIBIÇÃO
    try:
        periodo_exibicao = int(input("Período (dias) a ser exibido: "))
    except ValueError as e:
        periodo_exibicao = 90

    # ABRE A CONEXÃO COM O BANCO
    db = sqlite3.connect("cripto.db")
    cursor = db.cursor()
    print("#INFO: CONSULTANDO BASE DE DADOS...")
    time.sleep(1)

    # BUSCA AS INFORMAÇÕES DA MOEDA ESCOLHIDA
    cursor.execute("SELECT * FROM " + tabela + " ORDER BY DATA")
    historico = cursor.fetchall()
    colunas = [coluna[0] for coluna in cursor.description]
    resultado = pd.DataFrame(historico, columns=colunas).set_index("DATA")
    db.close()

    print("#INFO: CALCULANDO RSI...")
    time.sleep(1)
    data_frame = rsi(resultado, 14)

    filter_sell = (data_frame['RSI14'].shift(1) < 70) & (data_frame['RSI14'] > 70)
    data_frame.loc[filter_sell, 'Sell_Signal'] = data_frame.loc[filter_sell, 'FECHAMENTO']

    filter_buy = (data_frame['RSI14'].shift(1) > 30) & (data_frame['RSI14'] < 30)
    data_frame.loc[filter_buy, 'Buy_Signal'] = data_frame.loc[filter_buy, 'FECHAMENTO']

    data_atual = datetime.now().strftime("%Y-%m-%d")
    data_inicial = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    data_frame = data_frame.reset_index(drop=False)
    filtered_data = data_frame.loc[(data_frame['DATA'] < data_atual) & (data_frame['DATA'] > data_inicial)]
    filtered_data.loc[:, 'DATA'] = pd.to_datetime(filtered_data['DATA'], format='%Y-%m-%d')

    print("#INFO: MONTANDO O GRÁFICO...")
    time.sleep(1)

    plt.rcParams['figure.figsize'] = [22, 12]
    plt.rcParams['figure.dpi'] = 100

    fig, ax = plt.subplots()

    # Plot 1
    ax.plot(filtered_data['DATA'], filtered_data['FECHAMENTO'], label='Valor')

    # Plot 2
    ax2 = ax.twinx()
    ax2.plot(filtered_data['DATA'], filtered_data['RSI14'], label='RSI14', color='gray', alpha=0.5)
    ax2.axhline(70, color='gray', ls='--', alpha=0.5)
    ax2.axhline(30, color='gray', ls='--', alpha=0.5)
    ax2.set_ylabel('RSI14')

    ax.scatter(filtered_data['DATA'][filtered_data['Buy_Signal'].notna()],
               filtered_data['FECHAMENTO'][filtered_data['Buy_Signal'].notna()],
               label='Compra', marker='^', color='green', s=100)
    ax.scatter(filtered_data['DATA'][filtered_data['Sell_Signal'].notna()],
               filtered_data['FECHAMENTO'][filtered_data['Sell_Signal'].notna()],
               label='Venda', marker='v', color='red', s=100)

    ax.yaxis.set_major_formatter('${x:}')
    fig.autofmt_xdate()

    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))

    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    lines += lines2
    labels += labels2

    ax.set_title('Estratégia RSI')
    ax.set_ylabel('Valor')
    ax.set_xlabel('Data')
    ax.legend(lines, labels, loc='upper left')

    fig.savefig(f"charts/{tabela}.png")

    print("#INFO: PROCESSO FINALIZADO.")
