from updateDB import UpdateDB
import sqlite3
import time


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

    # ABRE A CONEXÃO COM O BANCO
    db = sqlite3.connect("cripto.db")
    cursor = db.cursor()
    print("#INFO: CONSULTANDO BASE DE DADOS...")
    time.sleep(1)

    # BUSCA AS INFORMAÇÕES DA MOEDA ESCOLHIDA
    cursor.execute("SELECT * FROM " + tabela + " ORDER BY DATA")
    historico = cursor.fetchall()
    db.close()
