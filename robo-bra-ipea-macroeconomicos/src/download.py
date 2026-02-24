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
from dateutil.relativedelta import relativedelta
from robo_api_lib import RoboApiLib
from ses_aws import SesAws

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
        description='Faz download dos Macroeconomicos pela API IPEA DATA.')
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
        "-m", 
        "--qtd_mes",
        required=False,
        help='quantidade de meses retroativos da data atual para captura',
        type=int,
        default=2
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
        '-id_macro',
        '--id_macro',
        help='Codigo da série IPEA a ser consultada. Caso Omitido, vai pegar default do config pelo robo-api'
    )
    parser.add_argument(
        '-u',
        '--base_url',
        default='https://www.ipeadata.gov.br/api/odata4/Metadados',
        required=True,
        help='Base url IPEA DATA'
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
    if not os.path.isdir(args.output_path):
        raise Exception(f'Invalid output dir: "{args.output_path}"')
    
    if not os.path.isdir(args.bkp_path):
        raise Exception(f'Invalid historico dir: "{args.bkp_path}"')
    
    ok, _ = get_data_ini(args)
    if not ok:
        raise Exception('Parameter data_ini with value invalid: "%s"', args.data_ini)

    ok, _ = get_data_fim(args)
    if not ok:
        raise Exception('Parameter data_fim with value invalid: "%s"', args.data_fim)



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
            data_ini = (datetime.datetime.today() - relativedelta(months=args.qtd_mes)).replace(day=1)
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

def api_headers():
    headers = {
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'Cache-Control': 'max-age=0',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
        'Content-Type': 'application/json; charset=utf-8'
    }
    
    return headers

def get_url_desc(args, cod_ipea):
    return f"{args.base_url}('{cod_ipea}')"

def get_url_price(args, cod_ipea):
    return f"{args.base_url}('{cod_ipea}')/Valores"

@retry(wait_fixed=10000,  # espera máxima de 10 segundos
       stop_max_attempt_number=10)  # tenta até 10 vezes
def _get_with_retry(session, url, **kwargs):
    return session.get(url, **kwargs)

def request_list(args, list_name, robo_name='robo-bra-ipea-macroeconomicos'):
    """ Request list from robo-api """
    try:
        logging.info(f'Loading list: "{list_name}" from: {robo_name}')
        robo_api = RoboApiLib(
            robo=robo_name,
            api_base=args.api_base,
            version=args.api_version,
            region=args.api_region,
            stage=args.api_stage
        )
        return robo_api.get_json_content(
            list_name=list_name,
            wait_timeout=args.api_timeout_secs
        )
    except:
        logging.exception(f'Error while requesting list: "{list_name}" from: {robo_name}')
        raise

def check_date(args, item):
    val_data = datetime.datetime.fromisoformat(item['VALDATA'])
    _, data_ini = get_data_ini(args)
    _, data_fim = get_data_fim(args)
    if (val_data.date() < data_ini.date() or val_data.date() > data_fim.date()):
        return True
    return False

def download(args):
    session = requests.Session()
    headers = api_headers()
    lista_macro = {}
    if args.id_macro:
        lista_macro['items'] = []
        lista_macro['items'].append({'cod_ipea': args.id_macro})  
    else:
        lista_macro = request_list(args, 'listademacroeconomicos')
    
    for macro in lista_macro['items']:
        url = get_url_desc(args, macro['cod_ipea'])
        resp = _get_with_retry(session, url, verify=False, headers=headers)
        if resp.status_code == 200:
            json_data = resp.json()
            data_update = datetime.datetime.fromisoformat(json_data['value'][0]['SERATUALIZACAO'])
            ind_name = json_data['value'][0]['SERCODIGO'].replace('_','-')
            filename = f"MACRO-IPEA-{ind_name}-{datetime.datetime.strftime(data_update,'%Y%m%d-%H%M')}.json"
            if already_process_file(args, filename):
                logging.info(f"Arquivo de {ind_name} com ultima atualização já foi baixada {filename}")
                continue
            
            url = get_url_price(args,macro['cod_ipea'])
            resp = _get_with_retry(session, url, verify=False, headers=headers)
            if resp.status_code == 200:
                json_data = resp.json()
                json_data['value'] = [item for item in json_data['value'] if not check_date(args,item)]
                if len(json_data['value']) > 0:
                    file_path = os.path.join(args.output_path, filename)
                    with open(file_path, mode='w', encoding='utf-8') as f:
                        json.dump(json_data, f, ensure_ascii=False)
            else:
                logging.error(f'status_code: {resp.status_code} response: {resp.text}')
                raise Exception('Falha ao capturar dados')
        else:
            logging.error(f'status_code: {resp.status_code} response: {resp.text}')
            raise Exception('Falha ao capturar dados')


if __name__ == '__main__':
    exit_code = 0
    try:
        configureLogging()
        args = parse_command_line()
        validate_args(args)
        download(args)
    except Exception as Ex:
        logging.exception(f'An unexpected exception was raised: {Ex}')
        exit_code = -1
    
    sys.exit(exit_code)