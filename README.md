# Pongo Financy

Robôs em Python que baixam **dados públicos do mercado financeiro brasileiro** e gravam os arquivos originais (JSON, XML ou TXT) numa pasta local, prontos para carga ou análise.

| Robô | Fonte | O que baixa |
|---|---|---|
| `robo-bcb-expectativa-mercado` | [Banco Central, Olinda/OData](https://dadosabertos.bcb.gov.br/dataset/expectativas-mercado) | Boletim Focus: expectativas de Selic, IPCA, PIB e câmbio (mensais, trimestrais, anuais, Top 5) |
| `robo-bra-ibge-indices` | [IBGE, API SIDRA](https://apisidra.ibge.gov.br/) | IPCA e INPC: variação no mês, no ano e em 12 meses |
| `robo-bra-ipea-macroeconomicos` | [IPEADATA](http://www.ipeadata.gov.br/api/) | Séries macro: câmbio, Selic over, petróleo Brent, Ibovespa |
| `robo-bra-b3-plantao-noticias` | [B3, Plantão de Notícias](https://sistemasweb.b3.com.br/PlantaoNoticias/Noticias/ListarTitulosNoticias) | Avisos de proventos e índices das empresas listadas |
| `robo-bra-cra-fnet` | [B3, FNET](https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCertificadosCVM) | Informes mensais de CRA |
| `robo-bra-cri-fnet` | B3, FNET | Informes mensais de CRI |
| `robo-fundo-bra-fnet` | B3, FNET | Informes mensais, trimestrais e anuais de FII e FIDC |

Todos os robôs:
- **são idempotentes**: um arquivo que já está na pasta de backup não é baixado de novo, a menos que você use `-f`;
- **tentam de novo** quando a fonte falha (os do BCB, IBGE, IPEA e fundos);
- **retornam código de saída diferente de zero** em caso de erro, o que facilita o agendamento (cron, Task Scheduler, Rundeck, Airflow).

## Como rodar

```bash
python -m venv .venv
.venv/Scripts/activate            # Linux/macOS: source .venv/bin/activate
pip install requests urllib3 retrying python-dateutil
mkdir saida backup
```

```bash
# Focus dos últimos 7 dias (pacotes padrão em config.json)
python robo-bcb-expectativa-mercado/src/download.py -o saida -b backup -q 7

# IPCA e INPC (tabelas em config.json)
python robo-bra-ibge-indices/src/download.py -o saida -b backup

# Séries do IPEADATA dos últimos meses (séries em config.json)
python robo-bra-ipea-macroeconomicos/src/download.py -o saida -b backup

# Proventos publicados no plantão de notícias da B3
python robo-bra-b3-plantao-noticias/src/download.py -o saida -b backup --id_agencia 18 --id_key PROVENTOS --file_prefix Proventos

# Informes mensais de CRA e CRI desde uma data (-l limita os downloads da execução)
python robo-bra-cra-fnet/download.py --file_prefix CRA --id_tipo_doc 81 -d 2026-10-01 -o saida -l 10
python robo-bra-cri-fnet/download.py --file_prefix CRI --id_tipo_fundo 5 --id_tipo_doc 86 -d 2026-10-01 -o saida -l 10

# Informes mensais dos FIIs em config.json (ou de um fundo específico com --cnpj)
python robo-fundo-bra-fnet/robo_download_fundo_bra_fnet.py --file_prefix FII --id_tipo_fundo 1 --id_tipo_doc 40 -d 2026-09-15 -o saida
```

Cada robô aceita `--help` com a lista completa de parâmetros. As listas padrão de pacotes, tabelas, séries e fundos ficam no `config.json` de cada pasta.

## Exemplo de saída

```
saida/
  ExpectativasMercadoSelic-20261016.json
  INDICES-IBGE-7060-20261009-090000.json
  MACRO-IPEA-GM366-ERC366-20261019-1548.json
  Proventos-3520302-19102026-085009.txt
  CRA-BRCASCCRA1M9-20260831-20261001-121200.xml
  FII-11728688000147-20260930-20261015-181900-1352479.xml
```
