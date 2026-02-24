## robo-fundo-bra-fnet/
- **robo_download_fundo_bra_fnet.py**: download de arquivos xml com informes do FNET
    - Configuração de cnpjs ignorados em config_fidc.py
    - **fonte:**
        https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM#
    
- **robo_extrator_fundo_bra_fnet_fidc.py**: extração dos dados cadastrais e informe mensal de FIDC do arquivo xml
    - Dados cadastrais extraidos: NR_CNPJ_ADM, TP_CONDOMINIO, FDO_EXCL, COTST_VINCUL
    - Todos dados de informe são extraidos
    - Configuração de campos para importação em config_fidc.py
