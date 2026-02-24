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
        description='Faz download dos indices IBGE pela API SIDRA.')
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
        '-id_table',
        '--id_table',
        help='ID tabela SIDRA a ser consultada. Caso Omitido, vai pegar default do config pelo robo-api'
    )
    parser.add_argument(
        '-fields',
        '--fields',
        help='IDs de campos variaveis com preços da tabela SIDRA a ser consultada. Caso Omitido, vai pegar default do config pelo robo-api'
    )
    parser.add_argument(
        '-id_group',
        '--id_group',
        help='ID do grupos da tabela SIDRA a ser consultada. Caso Omitido, vai pegar default do config pelo robo-api'
    )
    parser.add_argument(
        '-category_group',
        '--category_group',
        help='IDs de categorias do grupo da tabela SIDRA a ser consultada. Caso Omitido, vai pegar default do config pelo robo-api'
    )
    parser.add_argument(
        '-period',
        '--period',
        help='Período da tabela SIDRA a ser consultada. Caso Omitido, vai pegar período mais recente'
    )
    parser.add_argument(
        '-u',
        '--base_url',
        default='https://apisidra.ibge.gov.br',
        required=True,
        help='Base url IBGE SIDRA'
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

    return result

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

def get_url_desc_table(args, table):
    #Exemplo https://apisidra.ibge.gov.br/DescritoresTabela/t/6903     
    return f"{args.base_url}/DescritoresTabela/t/{table}"

def get_url_price(args, table, fields, group, period=None):
    #Exemplo https://apisidra.ibge.gov.br/values/t/7060/n1/1/v/63,69,66/c315/7170,7445/p/202507/h/n
    if period is None:
        period ="last"
    url = f"{args.base_url}/values/t/{table}/n1/1/p/{period}/v/{fields}"
    
    if group:
        url+= f"/{group['id']}/{group['category']}"
    
    url += "/h/n" #Sem header no resultado
     
    return url

@retry(wait_fixed=10000,  # espera máxima de 10 segundos
       stop_max_attempt_number=10)  # tenta até 10 vezes
def _get_with_retry(session, url, **kwargs):
    return session.get(url, **kwargs)

def get_config(args):
    try:
        logging.info('Carregando config de robo-bra-ibge-indices')
        robo_api = RoboApiLib(
            robo='robo-bra-ibge-indices',
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
        logging.exception('Falha ao carregar config de robo-bra-ibge-indices')
        raise

def already_process_file(args, filename):
    bkp_filename = os.path.join(args.bkp_path, filename)
    if os.path.isfile(bkp_filename) and (not args.force_download):
        return True
    return False


def download(args):
    if validate_args(args):
        session = requests.Session()
        headers = api_headers()
        config = {}
        if args.id_table and args.fields:
            config['table'] = []
            indice = {}
            indice['id'] = args.id_table
            indice['fields'] = args.fields
            if args.period:
                indice['period'] = args.period
            if args.id_group and args.category_group:
                indice['group'] = {"id": args.id_group, "category": args.category_group }
            config['table'].append(indice)  
        else:
            config = get_config(args)
        
        for indice in config['table']:
            url = get_url_desc_table(args, indice['id'])
            resp = _get_with_retry(session, url, verify=False, headers=headers)
            if resp.status_code == 200:
                json_data = resp.json()
                data_update = datetime.datetime.strptime(json_data['DataAtualizacao'],'%Y-%m-%d %H:%M:%S')
                filename = f"INDICES-IBGE-{indice['id']}-{datetime.datetime.strftime(data_update,'%Y%m%d-%H%M%S')}.json"
                if already_process_file(args, filename):
                    logging.info(f"Arquivo de {indice['name']} já foi baixada {filename}")
                    continue
                
                url = get_url_price(args, indice['id'], indice['fields'], indice.get('group',None), indice.get('period', None))
                resp = _get_with_retry(session, url, verify=False, headers=headers)
                if resp.status_code == 200:
                    json_data = resp.json()
                    if len(json_data) > 0:
                        file_path = os.path.join(args.output_path, filename)
                        with open(file_path, mode='w', encoding='utf-8') as f:
                            json.dump(json_data, f, ensure_ascii=False)
                else:
                    logging.error(f'status_code: {resp.status_code} response: {resp.text}')
                    raise Exception('Falha ao capturar dados')
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