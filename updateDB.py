import sqlite3
import yfinance as yf
import datetime
import time


# ATUALIZA AS INFORMAÇÕES NO BANCO
class UpdateDB:
    moedas = ["btc", "eth", "matic"]

    def __init__(self):
        try:
            print("#INFO: ATUALIZANDO BASE DE DADOS...")
            time.sleep(1)

            # ABRE A CONEXÃO COM O BANCO
            db = sqlite3.connect("cripto.db")
            cursor = db.cursor()

            try:
                # CRIA AS TABELAS CASO NÃO EXISTAM
                for moeda in self.moedas:
                    tabela = self.busca_nome_tabela(moeda)
                    nome_colunas = "DATA date, ABERTURA decimal, MINIMA decimal, MAXIMA decimal, FECHAMENTO decimal, " \
                                   "VOLUME decimal"
                    cursor.execute("CREATE TABLE IF NOT EXISTS " + tabela + " (" + nome_colunas + ")")
                    db.commit()

            except sqlite3.OperationalError as e:
                print("#ERRO: " + str(e))

            db.close()

            # ATUALIZA CADA MOEDA
            for moeda in self.moedas:
                tabela = self.busca_nome_tabela(moeda)
                ultima_data = self.busca_ultima_data_registrada(tabela)
                self.atualiza_valores_apos_ultima_data(self, moeda, tabela, ultima_data)

            print("#INFO: BASE DE DADOS ATUALIZADA!")
            time.sleep(1)

        except Exception as e:
            print(e)

    @staticmethod
    def busca_ultima_data_registrada(tabela):
        # ABRE A CONEXÃO COM O BANCO
        db = sqlite3.connect("cripto.db")
        cursor = db.cursor()

        # BUSCA A ÚLTIMA DATA REGISTRADA NO BANCO
        cursor.execute("SELECT DATA FROM " + tabela + " ORDER BY DATA desc")
        last_data = cursor.fetchall()
        db.close()
        return str(last_data[0][0]) if len(last_data) > 0 else ""

    @staticmethod
    def atualiza_valores_apos_ultima_data(self, symbol, tabela, ultima_data):
        try:
            # ABRE A CONEXÃO COM O BANCO
            db = sqlite3.connect("cripto.db")
            cursor = db.cursor()

            symbol_upper = symbol.upper()

            if ultima_data == "":
                # BUSCA OS VALORES
                cripto = yf.Ticker(symbol_upper + '-USD')
                historico = cripto.history(start=datetime.date(2010, 1, 1), end=None)
                lista_query_valores = self.monta_query_insert_valores(historico)

                try:
                    # INSERE AS INFORMAÇÕES NA TABELA
                    for valores in lista_query_valores:
                        query_campos = "DATA, ABERTURA, MINIMA, MAXIMA, FECHAMENTO, VOLUME"
                        cursor.execute("INSERT INTO " + tabela + " (" + query_campos + ") VALUES (" + valores + ")")

                    db.commit()

                    print("#INFO: TABELA " + tabela + " ATUALIZADA COM SUCESSO")
                    time.sleep(1)

                except sqlite3.OperationalError as e:
                    print("#ERRO: " + str(e))

            elif datetime.datetime.strptime(ultima_data, '%Y-%m-%d').date() == datetime.datetime.now().date():
                # BUSCA OS VALORES
                cripto = yf.Ticker(symbol_upper + '-USD')
                historico = cripto.history(start=datetime.datetime.now().date(), end=None)
                historico_hoje = historico.iloc[-1:]

                # ATUALIZA AS INFORMAÇÕES DE HOJE NA TABELA
                updates = "MINIMA = " + str(round(float(historico_hoje['Low'].iloc[0]), 2)) + ", "
                updates += "MAXIMA = " + str(round(float(historico_hoje['High'].iloc[0]), 2)) + ", "
                updates += "FECHAMENTO = " + str(round(float(historico_hoje['Close'].iloc[0]), 2)) + ", "
                updates += "VOLUME = " + str(round(float(historico_hoje['Volume'].iloc[0]), 2))

                try:
                    cursor.execute("UPDATE " + tabela + " SET " + updates + " WHERE DATA = (SELECT MAX(DATA) FROM "
                                                                            "ethereum)")
                    db.commit()

                    print("#INFO: TABELA " + tabela + " ATUALIZADA COM SUCESSO")
                    time.sleep(1)

                except sqlite3.OperationalError as e:
                    print("#ERRO: " + str(e))

            else:
                ultima_data_datetime = datetime.datetime.strptime(ultima_data, '%Y-%m-%d').date()
                ultima_data_datetime = ultima_data_datetime + datetime.timedelta(days=1)

                # BUSCA OS VALORES
                cripto = yf.Ticker(symbol_upper + '-USD')
                historico = cripto.history(start=ultima_data_datetime, end=None)
                lista_query_valores = self.monta_query_insert_valores(historico)

                try:
                    # INSERE AS INFORMAÇÕES NA TABELA
                    for valores in lista_query_valores:
                        query_campos = "DATA, ABERTURA, MINIMA, MAXIMA, FECHAMENTO, VOLUME"
                        cursor.execute("INSERT INTO " + tabela + " (" + query_campos + ") VALUES (" + valores + ")")

                    db.commit()

                    print("#INFO: TABELA " + tabela + " ATUALIZADA COM SUCESSO")
                    time.sleep(1)

                except sqlite3.OperationalError as e:
                    print("#ERRO: " + str(e))

            db.close()

        except Exception as e:
            print(e)

    @staticmethod
    def monta_query_insert_valores(historico):
        try:
            lista_query_valores = []

            lista_data = historico.index.tolist()
            lista_abertura = historico[['Open']].values.tolist()
            lista_minima = historico[['Low']].values.tolist()
            lista_maxima = historico[['High']].values.tolist()
            lista_fechamento = historico[['Close']].values.tolist()
            lista_volume = historico[['Volume']].values.tolist()

            for i in range(len(lista_data)):
                query_valores = "'" + str(lista_data[i].date()) + "', "
                query_valores += str(round(float(lista_abertura[i][0]), 2)) + ", "
                query_valores += str(round(float(lista_minima[i][0]), 2)) + ", "
                query_valores += str(round(float(lista_maxima[i][0]), 2)) + ", "
                query_valores += str(round(float(lista_fechamento[i][0]), 2)) + ", "
                query_valores += str(round(float(lista_volume[i][0]), 2))

                lista_query_valores.append(query_valores)

            return lista_query_valores

        except Exception as e:
            print(e)

    @staticmethod
    def busca_nome_tabela(symbol):
        tabela = "bitcoin"
        if symbol == "eth":
            tabela = "ethereum"
        elif symbol == "matic":
            tabela = "polygon"

        return tabela
