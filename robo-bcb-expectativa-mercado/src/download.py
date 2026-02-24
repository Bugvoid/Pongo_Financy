#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import logging
import argparse
import requests
import json
import datetime
import urllib3
from retrying import retry
from robo_api_lib import RoboApiLib

urllib3.disable_warnings()

def configureLogging():
    logFormatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.INFO)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)

    logging.info('inicializando processo "%s"', datetime.datetime.now().isoformat())
    logging.info('python "%s"', sys.version)
    logging.info('args: "%s"', sys.argv)

def parse_command_line():
    parser = argparse.ArgumentParser(
        description='Faz download das expectativas de mercado pelo Banco Central Brasil.')
    parser.add_argument(
        '-dt_ini',
        '--data_ini',
        help='Data inicial do arquivo a ser baixado YYYY-MM-DD. Ex: 2020-06-01. Caso Omitido, vai pegar a data atual',
        type=valid_date
    )
    parser.add_argument(
        '-dt_fim',
        '--data_fim',
        help='Data fim do arquivo a ser baixado YYYY-MM-DD. Ex: 2020-06-01. Caso Omitido, vai pegar a data atual',
        type=valid_date
    )
    parser.add_argument(
        "-q", 
        "--qtd_dias",
        required=False,
        help='quantidade de dias retroativos da data atual para captura',
        type=int,
        default=7
    )
    parser.add_argument(
        '-o',
        '--output_path',
        required=True,
        help='Pasta de saida para gravar o arquivo baixado.'
    )
    parser.add_argument(
        "-b",
        "--bkp_path",
        help="Pasta de Backup para os arquivos já baixados",
        required=True
    )
    parser.add_argument(
        '-pacotes',
        '--nomes_pacotes',
        help='Nomes dos itens de pacotes de dados da expectativas de mercado'
    )
    parser.add_argument(
        '-max',
        '--max_entid',
        required=True,
        type=int,
        help='Numero maximo de entidades que serão retornadas'
    )
    parser.add_argument(
        '-u',
        '--base_url',
        default='https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata',
        required=True,
        help='Base url Expectativa Mercado'
    )
    parser.add_argument(
        "-f", "--force_download",
        required=False,
        type=valid_boolean,
        help='Força download já processados anteriormente'
    )
    parser.add_argument(
        '-as',
        '--api_stage',
        required=True,
        help='API STAGE',
    )
    parser.add_argument(
        '-api',
        '--api_base',
        metavar='API_BASE',
        default='',
        help='Endereço da api de insumos de robos.'
    )
    parser.add_argument(
        '-av',
        '--api_version',
        metavar='API_VERSION',
        default=1,
        type=int,
        help='Versao da api de insumos de robos.'
    )
    parser.add_argument(
        '-apir',
        '--api_region',
        metavar='API_REGION',
        default='sa-east-1',
        help='Região da api de insumos de robos.'
    )
    parser.add_argument(
        '-at',
        '--api_timeout_secs',
        metavar='API_VERSION',
        default=60,
        type=int,
        help='Timeout de requisição do conteudo dos dados de ativos em segundos.'
    )
    
    return parser.parse_args()

def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Parâmetro de data inválida!: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def valid_boolean(s):
    try:
        return True if s == 'S' else False 
    except ValueError:
        msg = "Parâmetro de boolean inválida!: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def validate_args(args):
    result = True
    if not os.path.isdir(args.output_path):
        logging.error(f'pasta de saida "{args.output_path}" inválida')
        result = False
    
    if not os.path.isdir(args.bkp_path):
        logging.error(f'pasta de backup "{args.bkp_path}" inválida')
        result = False
    
    ok, _ = get_data_ini(args)
    if not ok:
        result = False

    ok, _ = get_data_fim(args)
    if not ok:
        result = False

    return result

def already_process_file(args, filename):
    bkp_filename = os.path.join(args.bkp_path, filename)
    if os.path.isfile(bkp_filename) and (not args.force_download):
        return True
    return False

def get_data_ini(args):
    ok = False
    data_ini = None
    try:
        data_ini = args.data_ini
        if not data_ini:
            data_ini = datetime.datetime.today() - datetime.timedelta(days=args.qtd_dias) 
        ok = True
    except ValueError:
        logging.error('Parameter data_ini with value invalid: "%s"', args.data_ini if args.data_ini else data_ini)

    return ok, data_ini

def get_data_fim(args):
    try:
        ok, data_ini = get_data_ini(args)
        if args.data_fim:
            data_fim = args.data_fim
        else:
            data_fim = datetime.datetime.today()

        if data_fim >= data_ini:
            return ok, data_fim
        else:
            return False, None
    except ValueError:
        logging.error('Parameter data_fim with value invalid: "%s"', args.data_fim)
        return False, None
    
def get_list_date(args):
    dates = []
    _, data_ini = get_data_ini(args)
    _, data_fim = get_data_fim(args)
    n_days = (data_fim - data_ini).days
    currente_date = datetime.datetime.now()
    
    for i in range(n_days):
        date = currente_date - datetime.timedelta(days=i)
        dates.append(date)
    
    return dates

def api_headers():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'Cache-Control': 'max-age=0',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36'
    }
    
    return headers

def get_url(args, pacote, date):
    orderby = 'indicador' if pacote == 'ExpectativasMercadoTop5Selic' else 'Indicador'
    url = f"{args.base_url}/{pacote}?$top={args.max_entid}&$filter=Data%20eq%20'{date.strftime('%Y-%m-%d')}'&$orderby={orderby}&$format=json"
     
    return url

@retry(wait_fixed=10000,  # espera máxima de 10 segundos
       stop_max_attempt_number=10)  # tenta até 10 vezes
def _get_with_retry(session, url, **kwargs):
    return session.get(url, **kwargs)

def get_config(args):
    try:
        logging.info('Carregando config de robo-bcb-expectativa-mercado')
        robo_api = RoboApiLib(
            robo='robo-bcb-expectativa-mercado',
            api_base=args.api_base,
            version=args.api_version,
            region=args.api_region,
            stage=args.api_stage
        )
        return robo_api.get_json_content(
            list_name='config',
            wait_timeout=args.api_timeout_secs
        )
    except:
        logging.exception('Falha ao carregar config de robo-bcb-expectativa-mercado')
        raise

def download(args):
    if validate_args(args):
        session = requests.Session()
        headers = api_headers()
        config = get_config(args)
        if args.nomes_pacotes:
            list_pacotes = args.nomes_pacotes.split(',')
        else:
            list_pacotes = config['pacotes']
        list_date = get_list_date(args)
        for data_expectativa in list_date:
            for item in list_pacotes: 
                pacote = item.strip()
                filename = f"{pacote}-{data_expectativa.strftime('%Y%m%d')}.json"
                if already_process_file(args, filename):
                    logging.info(f'Arquivo de {pacote} já foi baixada {filename}')
                    continue
                
                url = get_url(args, pacote, data_expectativa)
                resp = _get_with_retry(session, url, verify=False, headers=headers)
                if resp.status_code == 200:
                    json_data = resp.json()
                    if len(json_data['value']) > 0:
                        file_path = os.path.join(args.output_path, filename)
                        with open(file_path, mode='w', encoding='utf-8') as f:
                            json.dump(json_data, f, ensure_ascii=False)
                else:
                    logging.error(f'status_code: {resp.status_code} response: {resp.text}')
                    raise Exception('Falha ao capturar dados')
    else:
        raise Exception(f'Argumentos invalidos!')

if __name__ == '__main__':
    exit_code = 0
    try:
        configureLogging()
        args = parse_command_line()
        download(args)
    except Exception as Ex:
        logging.exception(f'An unexpected exception was raised: {Ex}')
        exit_code = -1
    
    sys.exit(exit_code)