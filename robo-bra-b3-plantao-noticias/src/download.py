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
import time
import random
from retrying import retry
import re
import html

urllib3.disable_warnings()

PAGINA_INICIO_COOKIE_TOKEN = 'https://sistemasweb.b3.com.br/PlantaoNoticias/Noticias/Index'
URL_PLANTAO_NOTICIAS = 'https://sistemasweb.b3.com.br/PlantaoNoticias/Noticias/ListarTitulosNoticias'
# https://sistemasweb.b3.com.br/PlantaoNoticias/Noticias/ListarTitulosNoticias?agencia=18&palavra=Proventos&dataInicial=2025-07-01&dataFinal=2025-07-11
URL_DOWNLOAD_NOTICIA = 'https://sistemasweb.b3.com.br/PlantaoNoticias/Noticias/Detail'
#https://sistemasweb.b3.com.br/PlantaoNoticias/Noticias/Detail?idNoticia=3002381&agencia=18&dataNoticia=2025-07-11%2009:50:11
EXTENSAO = 'txt'
TIMEOUT = (15, 120)  # (conexão, leitura) em segundos
# A API devolve tudo de uma vez (sem paginação) e responde corpo vazio
# quando o período passa de ~92 dias.
MAX_DIAS_PERIODO = 90

def parse_command_line():
    parser = argparse.ArgumentParser(
        description='Faz download das noticias no plantão de noticias da b3.')
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
        '-o',
        '--output_path',
        required=True,
        help='Pasta de saida para gravar o arquivo baixado.'
    )
    parser.add_argument(
        "-b",
        "--bkp_path",
        help="Backup directory for files already extracted",
        required=True
    )
    parser.add_argument(
        '-id_ag',
        '--id_agencia',
        required=True,
        help='Tipo de agencia para pesquisa de noticia.'
    )
    parser.add_argument(
        '-id_k',
        '--id_key',
        required=True,
        help='Palavra chave para pesquisa de noticias.'
    )
    parser.add_argument(
        "-prefix", 
        "--file_prefix",  # Tipo de noticia
        required=True,
        type=str,
        help='Define prefixo do arquivo a ser salvo no download. Ex: Corporate = Corporate_20240702'
    )
    parser.add_argument(
        "-f", "--force_download",
        required=False,
        nargs='?',
        const=True,
        default=False,
        type=valid_boolean,
        help='Força download já processados anteriormente (-f ou -f S)'
    )
    
    return parser.parse_args()

def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Parâmetro de data inválida!: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def valid_boolean(s):
    if isinstance(s, bool):
        return s
    v = str(s).strip().upper()
    if v in ('S', 'SIM', 'Y', 'YES', 'TRUE', '1'):
        return True
    if v in ('N', 'NAO', 'NÃO', 'NO', 'FALSE', '0'):
        return False
    raise argparse.ArgumentTypeError("Parâmetro de boolean inválido!: '{0}'. Use S ou N.".format(s))

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

    ok, data_fim = get_data_fim(args)
    if not ok:
        logging.error('data_fim menor que data_ini (ou data_ini no futuro sem data_fim)')
        result = False
    else:
        _, data_ini = get_data_ini(args)
        if (data_fim - data_ini).days > MAX_DIAS_PERIODO:
            logging.error(f'Período de {(data_fim - data_ini).days} dias: a B3 só responde até ~{MAX_DIAS_PERIODO} dias por consulta')
            result = False

    return result

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


def write_file(text, filename):
    with open(filename, 'w', encoding="utf-8") as f:
        f.write(text)
    logging.info('Arquivo "%s" gravado', filename)

def already_process_file(args, filename):
    bkp_filename = os.path.join(args.bkp_path, filename)
    if os.path.isfile(bkp_filename) and (not args.force_download):
        return os.stat(bkp_filename).st_size > 0
    return False

def nome_arquivo(prefix, id_noticia, data_pub):
    return '-'.join([
        prefix,
        str(id_noticia),
        data_pub.strftime('%d%m%Y-%H%M%S'),
    ]) + '.' + EXTENSAO

def get_data_ini(args):
    data_ini = args.data_ini
    try:
        if not data_ini:
            data_ini = datetime.datetime.today()

        return True, data_ini
    except ValueError:
        logging.error('Parameter data_ini with value invalid: "%s"', args.data_ini if args.data_ini else data_ini)
        return False, None

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
            return False, None  # não aceitar se até for menor a data_ini
    except ValueError:
        logging.error('Parameter data_fim with value invalid: "%s"', args.data_fim)
        return False, None
    
def make_params_request(args):

    _, _ini = get_data_ini(args)
    _, _fim = get_data_fim(args)
    
    params = {
        'agencia': args.id_agencia,
        'palavra': args.id_key,
        'dataInicial': _ini.strftime('%Y-%m-%d'),
        'dataFinal': _fim.strftime('%Y-%m-%d'),
    }

    return params

def make_api_headers():
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'Cache-Control': 'private',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36'
    }

    return headers

def match(text):
        return re.search(r"<pre[^>]*>(?P<text>.*?)</pre>", text, re.DOTALL | re.IGNORECASE)

@retry(wait_fixed=10000,  # espera máxima de 10 segundos
       stop_max_attempt_number=10)  # tenta até 10 vezes
def _get_with_retry(session, url, **kwargs):
    kwargs.setdefault('timeout', TIMEOUT)
    resp = session.get(url, **kwargs)
    if resp.status_code >= 500:
        raise requests.HTTPError(f'{resp.status_code} em {url}')  # dispara nova tentativa
    return resp


def get_session():
    session = requests.Session()
    first_headers = make_api_headers()
    resp = _get_with_retry(session, PAGINA_INICIO_COOKIE_TOKEN, verify=False, headers=first_headers)

    if resp.status_code == 200:
        return session, resp
    else:
        return None, None
    
def get_json_download(args, session):
    """A API não pagina: uma chamada traz todos os itens do período."""
    result = {'data': []}

    json_request = make_params_request(args)
    resp_json = _get_with_retry(
        session,
        URL_PLANTAO_NOTICIAS,
        verify=False, headers=make_api_headers(), params=json_request
    )

    if resp_json.status_code != 200:
        raise Exception(f'Listagem retornou status {resp_json.status_code}. request: {json_request}')

    if not resp_json.text.strip():
        raise Exception(f'Listagem retornou corpo vazio (período longo demais?). request: {json_request}')

    try:
        data = json.loads(resp_json.text)
    except ValueError as xcp:
        raise Exception(f'Listagem não retornou JSON válido: {resp_json.text[:200]!r}') from xcp

    if not data:
        logging.warning(f'Pesquisa não encontrou dados para o filtro: {json_request}')
        return result

    vistos = set()
    for item in data:
        id_noticia = item['NwsMsg']['id']
        if id_noticia not in vistos:
            vistos.add(id_noticia)
            result['data'].append(item)

    logging.info(f'{len(result["data"])} notícias encontradas para download.')
    return result

def download(args):
    """Retorna a quantidade de notícias que falharam."""
    if not validate_args(args):
        raise Exception('Argumentos invalidos!')

    session, _ = get_session()
    if not session:
        raise Exception('Erro ao iniciar pagina da sessao e cookies.')

    lista_para_download = get_json_download(args, session)

    falhas = 0
    for download_item in lista_para_download['data']:
        noticia = download_item['NwsMsg']
        arquivo = nome_arquivo(
            args.file_prefix,
            noticia['id'],
            datetime.datetime.strptime(noticia['dateTime'], "%Y-%m-%d %H:%M:%S")
        )
        if already_process_file(args, arquivo):
            logging.info(f'Noticia já foi baixada {arquivo}')
            continue

        # Pausa aleatória entre downloads para não ser bloqueado
        time.sleep(random.uniform(0.5, 2))
        try:
            response = _get_with_retry(
                session,
                URL_DOWNLOAD_NOTICIA,
                params={'idNoticia': noticia['id'], 'agencia': noticia['IdAgencia'], 'dataNoticia': noticia['dateTime']},
                verify=False,
                headers=make_api_headers()
            )
        except Exception as xcp:
            logging.error(f'Falha no download da notícia {noticia["id"]}: {xcp}')
            falhas += 1
            continue

        if response.status_code != 200:
            logging.warning('Notícia %s: status code %s', noticia['id'], response.status_code)
            falhas += 1
            continue

        m = match(response.text)
        if not m:
            logging.warning('Notícia %s: página sem bloco <pre>, nada gravado', noticia['id'])
            falhas += 1
            continue

        logging.info(f'Retorno ok, gerando o arquivo {arquivo}')
        write_file(html.unescape(m.group('text')), os.path.join(args.output_path, arquivo))

    return falhas


if __name__ == '__main__':
    try:
        configureLogging()
        args = parse_command_line()
        falhas = download(args)
    except Exception as Ex:
        logging.exception(f'An unexpected exception was raised: {Ex}')
        sys.exit(1)
    if falhas:
        logging.error(f'{falhas} notícia(s) não foram baixadas')
        sys.exit(2)