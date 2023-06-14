import sqlite3
import yfinance as yf
import datetime


# ATUALIZA AS INFORMAÇÕES NO BANCO
class UpdateDB:
    @staticmethod
    def cria_tabelas():
        # ABRE A CONEXÃO COM O BANCO
        db = sqlite3.connect("cripto.db")
        cursor = db.cursor()

        # CRIA AS TABELAS CASO NÃO EXISTAM
        cursor.execute("CREATE TABLE IF NOT EXISTS bitcoin (BTC_DATA date, BTC_VALOR decimal)")
        cursor.execute("CREATE TABLE IF NOT EXISTS ethereum (ETH_DATA date, ETH_VALOR decimal)")
        cursor.execute("CREATE TABLE IF NOT EXISTS polygon (MATIC_DATA date, MATIC_VALOR decimal)")

        db.commit()
        db.close()

    @staticmethod
    def busca_ultima_data_registrada(symbol):
        # ABRE A CONEXÃO COM O BANCO
        db = sqlite3.connect("cripto.db")
        cursor = db.cursor()

        tabela = "bitcoin"
        if symbol == "eth":
            tabela = "ethereum"
        if symbol == "matic":
            tabela = "polygon"

        symbol_upper = symbol.upper()

        # BUSCA A ÚLTIMA DATA REGISTRADA NO BANCO
        cursor.execute("SELECT " + symbol_upper + "_DATA FROM " + tabela + " ORDER BY " + symbol_upper + "_DATA desc")
        last_data = cursor.fetchall()
        db.close()
        return str(last_data[0][0]) if len(last_data) > 0 else ""

    @staticmethod
    def atualiza_valores_apos_ultima_data(symbol, ultima_data):
        # ABRE A CONEXÃO COM O BANCO
        db = sqlite3.connect("cripto.db")
        cursor = db.cursor()

        tabela = "bitcoin"
        if symbol == "eth":
            tabela = "ethereum"
        if symbol == "matic":
            tabela = "polygon"

        symbol_upper = symbol.upper()

        if ultima_data == "":
            # BUSCA OS VALORES
            cripto = yf.Ticker(symbol_upper + '-USD')
            historico = cripto.history(start=datetime.date(2010, 1, 1), end=None)

            valores = historico[['Close']].values.tolist()
            datas = historico.index.tolist()

            # INSERE AS INFORMAÇÕES NA TABELA
            for i in range(len(datas)):
                query_campos = symbol_upper + "_DATA, " + symbol_upper + "_VALOR"
                query_valores = "'" + str(datas[i].date()) + "', " + str(round(float(valores[i][0]), 2))
                cursor.execute("INSERT INTO " + tabela + " (" + query_campos + ") VALUES (" + query_valores + ")")

            db.commit()

            print("#INFO: TABELA " + tabela + " ATUALIZADA COM SUCESSO")

        elif datetime.datetime.strptime(ultima_data, '%Y-%m-%d').date() == datetime.datetime.now().date():
            return

        else:
            ultima_data_datetime = datetime.datetime.strptime(ultima_data, '%Y-%m-%d').date()
            ultima_data_datetime = ultima_data_datetime + datetime.timedelta(days=1)

            # BUSCA OS VALORES
            cripto = yf.Ticker(symbol_upper + '-USD')
            if ultima_data_datetime == datetime.datetime.now().date():
                historico = cripto.history(start=ultima_data_datetime, end=None)
            else:
                historico = cripto.history(start=ultima_data_datetime, end=datetime.datetime.now().date())

            valores = historico[['Close']].values.tolist()
            datas = historico.index.tolist()

            # INSERE AS INFORMAÇÕES NA TABELA
            for i in range(len(datas)):
                query_campos = symbol_upper + "_DATA, " + symbol_upper + "_VALOR"
                query_valores = "'" + str(datas[i].date()) + "', " + str(round(float(valores[i][0]), 2))
                cursor.execute("INSERT INTO " + tabela + " (" + query_campos + ") VALUES (" + query_valores + ")")

            db.commit()

            print("#INFO: TABELA " + tabela + " ATUALIZADA COM SUCESSO")

        db.close()
