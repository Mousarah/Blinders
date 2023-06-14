from updateDB import UpdateDB


if __name__ == "__main__":
    print("#INFO: ATUALIZANDO BASE DE DADOS...")

    UpdateDB.cria_tabelas()
    ultima_data_btc = UpdateDB.busca_ultima_data_registrada("btc")
    UpdateDB.atualiza_valores_apos_ultima_data("btc", ultima_data_btc)
    ultima_data_eth = UpdateDB.busca_ultima_data_registrada("eth")
    UpdateDB.atualiza_valores_apos_ultima_data("eth", ultima_data_eth)
    ultima_data_matic = UpdateDB.busca_ultima_data_registrada("matic")
    UpdateDB.atualiza_valores_apos_ultima_data("matic", ultima_data_matic)

    print("#INFO: BASE DE DADOS ATUALIZADA")
