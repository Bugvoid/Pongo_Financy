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
import boto3
import tempfile
import time
import random
import dados.lista_de_fundos as lista_de_fundos
import re
from retrying import retry

'''
# suprimir mensagens de que nao usa certificado nas requisicoes https
InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised.
See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
'''
urllib3.disable_warnings()

PAGINA_INICIO_COOKIE_TOKEN = 'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM#'
URL_DOWNLOAD_XML = 'https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento'
URL_PESQUSA_GERENCIADOR_DOCUMENTOS = 'https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados'  # página do grid inicial
# d=5&s=0&l=10&o[0][dataEntrega]=desc&tipoFundo=1&cnpjFundo=13555918000149&cnpj=13555918000149&idCategoriaDocumento=0&idTipoDocumento=0&idEspecieDocumento=0&dataInicial=23/01/2020&dataFinal=24/01/2020&_=1579893315533

MAX_RESULT_API = 200


def configureLogging():
    logFormatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.INFO)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)

    logging.info('args: "%s"', sys.argv)


def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def cnpj_alfanumerico(cnpj):
    return re.sub(r'\W+','', cnpj.strip())

def _validate_cnpj(cnpj):
    if cnpj:
        cnpj_valid = cnpj_alfanumerico(cnpj)
        if (len(cnpj_valid) != 14):
            return None

    return cnpj

def format_cnpj_with_separator(cnpj):
    format_cnpj = cnpj_alfanumerico(cnpj)
    return f"{format_cnpj[:2]}.{format_cnpj[2:5]}.{format_cnpj[5:8]}/{format_cnpj[8:12]}-{format_cnpj[12:]}"

def create_parser():
    parse = argparse.ArgumentParser()

    parse.add_argument(
        "-prefix", "--file_prefix",  # Tipo do fundo
        required=True,
        type=str,
        help='Define prefixo do arquivo a ser salvo no download. Ex: FII = FII-8924783000101-20200131-20200128-114809.xml'
    )

    parse.add_argument(
        "-id_tipo", "--id_tipo_fundo",  # TIPO_FUNDO
        required=True,
        type=int,
        help='Id do tipo de fundo do campo em "extrair inf. mensal" do fnet. 1=Fundo Imobiliario,2=FIDIC'
    )

    parse.add_argument(
        "-id_cat", "--id_categoria_doc",  # ID_CATEGORIA_DOCUMENTO
        required=False,
        type=int,
        default=6,
        help='Id da categoria documento do campo em "extrair inf. mensal" do fnet. 6=informes periodicos'
    )

    parse.add_argument(
        "-id_doc", "--id_tipo_doc",  # ID_TIPO_DOCUMENTO
        required=True,
        type=int,
        help='Id do tipo documento do campo em "extrair inf. mensal" do fnet. 40=informe mensal, 45=informe trimestral, 47=informe anual'
    )

    parse.add_argument(
        "-id_esp", "--id_especie_doc",  # ID_ESPECIE_DOCUMENTO
        required=False,
        type=int,
        default=0,  # 0=todos
        help='Id da espécie documento do campo em "extrair inf. mensal" do fnet. 0=todos'
    )

    parse.add_argument(
        "-c", "--cnpj",  # exemplo: 16915840000114
        required=False,
        help='cnpj para baixar unico. Se informado ignora lista do arquivo.',
        type=_validate_cnpj
    )

    parse.add_argument(
        "-q", "--qtd_dias",
        required=False,
        help='quantidade de dias retroativos da data atual para capturar informe',
        type=int,
        default=7  # padrão.
    )

    parse.add_argument(
        "-d", "--data_de",  # exemplo: 2017-10-02
        required=False,
        help='data inicial para download dos arquivos. se nao informado assume hoje menos default de dias pra tras.',
        type=valid_date
    )
    parse.add_argument(
        "-ate", "--data_ate",
        required=False,
        default=None,
        nargs='?',
        help='data até. Se não informado, assume o dia de hoje.',
        type=valid_date
    )
    parse.add_argument(
        "-o", "--output_path",
        help='caminho para o arquivo baixado'
    )
    parse.add_argument(
        "-bucket", "--bucket_name",
        help='nome do bucket do s3 onde salvara arquivo'
    )
    parse.add_argument(
        "-bucket_prefix", "--bucket_prefix",
        help='diretório do bucket do s3 onde salvara arquivo'
    )
    parse.add_argument(
        '-api',
        '--api_base',
        metavar='API_BASE',
        default='',
        help='Endereço da api de insumos de robos.'
    )
    parse.add_argument(
        '-av',
        '--api_version',
        metavar='API_VERSION',
        default=1,
        type=int,
        help='Versao da api de insumos de robos.'
    )
    parse.add_argument(
        '-ar',
        '--api_region',
        metavar='API_REGION',
        default='sa-east-1',
        help='Região da api de insumos de robos.'
    )
    parse.add_argument(
        '-at',
        '--api_timeout_secs',
        metavar='API_VERSION',
        default=60,
        type=int,
        help='Timeout de requisição do conteudo dos dados de ativos em segundos.'
    )
    parse.add_argument(
        "-cnpj_separadores", "--cnpj_com_separadores",
        action='store_true',
        default=False,
        help='Permite o cnpj com os separadores para baixar o informe'
    )
    return parse


def _get_data_de(args):
    '''
    data incial: caso nao informada assume o dia atual retroativo a quantidade de dias do robo.
    '''
    data_de = args.data_de
    try:
        if not data_de:
            data_de = datetime.datetime.today() - datetime.timedelta(days=args.qtd_dias)

        return True, data_de
    except ValueError:
        logging.error('parameter data_de with value invalid: "%s"', args.data_de if args.data_de else data_de)
        return False, None


def _get_data_ate(args):

    try:
        ok, data_de = _get_data_de(args)
        if args.data_ate:
            data_ate = args.data_ate
        else:
            data_ate = datetime.datetime.today()

        if data_ate >= data_de:
            return ok, data_ate
        else:
            return False, None  # não aceitar se até for menor a data_de
    except ValueError:
        logging.error('parameter data_ate with value invalid: "%s"', args.data_ate)
        return False, None


def validate_args(args):

    ok, data = _get_data_de(args)
    if not ok:
        return False  # Erro

    ok, data = _get_data_ate(args)
    if not ok:
        return False  # Erro

    if args.cnpj and args.cnpj.strip():
        ok = _validate_cnpj(args.cnpj)
        if not ok:
            return False

    if (args.output_path) and ((not os.path.exists(args.output_path)) or (not os.path.isdir(args.output_path))):
        logging.error('parâmetro output directory "%s" not exist', args.output_path)
        return False  # Erro
    elif (not args.output_path):
        if (not args.bucket_name):
            logging.error('parâmetro bucket_name não informado')
            return False  # Erro
        if (not args.bucket_prefix):
            logging.error('parâmetro bucket_prefix não informado')
            return False  # Erro

    if (not args.file_prefix) or (not args.file_prefix.strip()):
        logging.error(f'file_prefix é inválido "{args.file_prefix}".')

    return True  # Tudo Ok


def parseargs():
    parse = create_parser()
    return parse.parse_args()


def write_file(text, filename):
    with open(filename, 'wb') as f:
        f.write(text)
    logging.info('file "%s" gravado', filename)


def nome_arquivo(tipo_fundo, item, data_ref, data_pub, extensao):
    if extensao not in ['pdf', 'xml']:
        extensao = 'xml'
    '''
        # formato original: <carga format-output-fileName="FII-%1$tY%1$tm%1$td-%1$tk%1$tM%1$tS.fii">
        # Atual:
        # Formato: FII-8924783000101-20200131-20200128-114809-110294.xml
        # Formato: TipoFundo-CNPJ-YYYYMMDD-YYYYMMDD-HMS
    '''
    return '-'.join([
        tipo_fundo,
        cnpj_alfanumerico(item['cnpj']).zfill(14),
        data_ref.strftime('%Y%m%d'),
        data_pub.strftime('%Y%m%d-%H%M%S')        
    ]) + '.' + extensao


def get_fundos_to_download(args, stage):
    if args.cnpj:
        result = [{'cnpj': str(args.cnpj)}]
    else:
        result = []
        if args.id_tipo_fundo == 1:  # Fundo Imobiliario
            result = lista_de_fundos.get_fundos_pra_imp_bal_from_api(
                args=args,
                stage=stage,
                cnpjs_ignorados=[]
            )
        elif args.id_tipo_fundo == 2:  # FIDC
            result = lista_de_fundos.get_fundos_fidc_from_api(
                args=args,
                stage=stage,
                cnpjs_ignorados=[]
            )

    return result


def _make_params_request(args, cnpj, start=0, limit=MAX_RESULT_API):

    ok, _de = _get_data_de(args)
    ok, _ate = _get_data_ate(args)
    cnpjfundo = str(cnpj) if args.cnpj_com_separadores else str(int(cnpj))
    
    params = {
        'd': '0',
        's': str(start),
        'l': str(limit),
        'o[0][dataEntrega]': 'asc',
        'tipoFundo': str(args.id_tipo_fundo),
        'cnpjFundo': cnpjfundo,
        'cnpj': cnpjfundo,
        'idCategoriaDocumento': str(args.id_categoria_doc),
        'idTipoDocumento': str(args.id_tipo_doc),
        'idEspecieDocumento': str(args.id_especie_doc),
        'situacao': 'A',  # somente os documentos ATIVOs
        'dataInicial': _de.strftime('%d/%m/%Y'),
        'dataFinal': _ate.strftime('%d/%m/%Y'),
        '_': str(datetime.datetime.today().timestamp())  # toda request unica
    }

    return params


def _make_headers_first_request():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Host': 'fnet.bmfbovespa.com.br',
        'Pragma': 'no-cache',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1'
    }

    return headers


def _make_api_headers(last_response, cnpj):
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Host': 'fnet.bmfbovespa.com.br',
        'Pragma': 'no-cache',
        'Referer': 'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo=' + cnpj,
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }

    if "CSRFToken" in last_response.headers:
        headers["CSRFToken"] = last_response.headers["CSRFToken"]

    if "Cookie" in last_response.headers:
        headers["Cookie"] = last_response.headers["Cookie"]

    return headers


def _make_download_headers(last_response):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Host': 'fnet.bmfbovespa.com.br',
        'Pragma': 'no-cache',
        'Referer': 'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Upgrade-Insecure-Requests': '1'
    }

    if last_response is not None:
        if "CSRFToken" in last_response.headers:
            headers["CSRFToken"] = last_response.headers["CSRFToken"]

        if "Cookie" in last_response.headers:
            headers["Cookie"] = last_response.headers["Cookie"]
    else:
        logging.warning('last_response is None')

    return headers


# @retry em conjunto com _get_with_retry é decorator do requests para aplicar o retry em conjunto com o @retry configurado anteriormente
# detalhe que o def _get_with_retry precisa ficar imediatamente em seguida ao @retry  para funcionar, regra do python
@retry(wait_fixed=10000,  # espera máxima de 10 segundos
       stop_max_attempt_number=10)  # tenta até 10 vezes
def _get_with_retry(session, url, **kwargs):
    return session.get(url, **kwargs)


def _get_session():
    session = requests.Session()
    first_headers = _make_headers_first_request()
    resp = _get_with_retry(session, PAGINA_INICIO_COOKIE_TOKEN, verify=False, headers=first_headers)

    if resp.status_code == 200:
        return session, resp
    else:
        return None, None


def get_json_download(args, arg_cnpj):
    cnpj = _validate_cnpj(arg_cnpj)
    if cnpj is None:
        logging.warning(f'CNPJ is invalid: "{arg_cnpj}"')
        return None

    result = {
        'data': []
    }

    start = 0  # paginacao
    continua = True

    session, resp = _get_session()

    if session and resp:
        recordsFiltered = 0
        recordsTotal = 0
        data = None
        msg = None
        while continua:
            # Adiciona um atraso aleatório entre as requisições para não ser bloqueado
            time.sleep(random.randint(0, 3))

            json_request = _make_params_request(args, cnpj, start)
            req_header = _make_api_headers(resp, cnpj)
            resp_json = _get_with_retry(
                session,
                URL_PESQUSA_GERENCIADOR_DOCUMENTOS,
                verify=False, headers=req_header, params=json_request
            )

            if resp_json.status_code == 200:
                json_res = json.loads(resp_json.text)
                recordsFiltered = json_res["recordsFiltered"] if 'recordsFiltered' in json_res else 0
                recordsTotal = json_res["recordsTotal"] if 'recordsTotal' in json_res else 0
                msg = json_res["msg"] if 'msg' in json_res else None
                data = json_res["data"] if 'data' in json_res else None

                if ((recordsFiltered == 0) or (recordsTotal == 0)):
                    continua = False
                    logging.warning(f'40055 - Pesquisa não retornou dados para o filtro: args {args} e cnpj {str(cnpj)}')
                    return result
                if msg:
                    continua = False
                    logging.warning(f'40056 - Pagina retornou a mensagem {msg} para o filtro: args {args} e cnpj {str(cnpj)}')
                    return result

                if not data:
                    continua = False
                    logging.warning(f'40057 - Pagina não retornou dados para o filtro: {args} e cnpj {str(cnpj)}')
                    return result

                # Para testes simulei 2253 retornos no data a serem concatenados
                # https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?d=0&s=2252&l=200&o[0][dataEntrega]=desc&tipoFundo=1&idCategoriaDocumento=6&idTipoDocumento=4&idEspecieDocumento=0&dataInicial=01/01/2000&dataFinal=27/01/2020&_=1580150474305

                # concatena todas as listagens
                if data and len(data) > 0:
                    result['data'].extend(data)
            else:
                continua = False
                logging.error(f'Erro ao pegar json da paginação atual. request: {json_request}, args: {args} e cnpj {str(cnpj)}')
                return result

            if recordsTotal > MAX_RESULT_API:
                if start < recordsTotal:
                    start = (start + MAX_RESULT_API)
            else:
                logging.info(f'Tentou-se extrair  {recordsTotal} itens para download.')
                continua = False
    else:
        logging.error('Erro ao iniciar pagina da sessao e cookies.')

    if not result['data']:
        logging.warning(f'Pesquisa não encontrou dados para argumentos: {args} , cnpj {str(cnpj)}, start {start}.')

    return result


def save_content_s3(s3_client, bucket, key, bcontent):

    temp_file = tempfile.TemporaryFile()

    try:
        temp_file.write(bcontent)
        temp_file.seek(0)

        s3_client.upload_fileobj(temp_file, bucket, key, ExtraArgs={'ACL':'bucket-owner-full-control'})
        logging.info(f'Arquivo gravado s3:{bucket}/{key}')

    finally:
        temp_file.close()

    return True


def download(args, stage):
    if validate_args(args):

        if args.cnpj:
            list_cnpjs = [{'cnpj': str(args.cnpj)}]
        else:
            list_cnpjs = get_fundos_to_download(args, stage)

        if list_cnpjs and len(list_cnpjs) > 0:

            s3_client = boto3.client('s3')
            countException = 0
            
            for item in list_cnpjs:
                if args.cnpj_com_separadores:
                    cnpj = format_cnpj_with_separator(item['cnpj'])
                else:
                    cnpj = item['cnpj']
                
                try:
                    lista_para_download = get_json_download(args, cnpj)
                except Exception as xcp:
                    logging.exception('Except durante o download do arquivo CNPJ:"%s", Exception:"%s"', str(cnpj), xcp)
                    lista_para_download = None
                    countException += 1

                if (lista_para_download is not None) and lista_para_download['data'] and len(lista_para_download['data']) > 0:
                    session, resp = _get_session()
                    for download_item in lista_para_download['data']:
                        req_header = _make_download_headers(resp)
                        try:
                            response = _get_with_retry(
                                session,
                                URL_DOWNLOAD_XML,
                                params={'id': download_item['id']},
                                verify=False,
                                headers=req_header
                            )
                        except Exception as xcp:
                            logging.exception('Except durante o download do arquivo CNPJ:"%s", URL:"%s", download_item["id"]:"%s", Exception:"%s"', str(cnpj), str(URL_DOWNLOAD_XML), str(download_item["id"]), xcp)
                            response = None
                            countException += 1

                        if not(response is None):
                            if (response.status_code == 200):
                                extensao = 'xml'
                                if "Content-Disposition" in response.headers:
                                    # 'Content-Disposition']: attachment; filename="16915840000114-IFP11012017V01-000008070.pdf"
                                    filename = response.headers['Content-Disposition']
                                    extensao = filename[-4:-1]

                                data_pub = datetime.datetime.strptime(download_item['dataEntrega'], '%d/%m/%Y %H:%M')
                                if(len(download_item['dataReferencia']) > 7):
                                    data_ref = datetime.datetime.strptime(download_item['dataReferencia'], '%d/%m/%Y')

                                arquivo = nome_arquivo(
                                    args.file_prefix,
                                    item,
                                    data_ref,
                                    data_pub,
                                    str(download_item['id']), #Id obrigatóriamente deve fazer parte do nome do arquivo 
                                    extensao
                                )
                                logging.info(f'Retorno ok, gerando o arquivo {arquivo}')
                                if args.output_path:
                                    write_file(
                                        response.content,
                                        os.path.join(
                                            args.output_path,
                                            arquivo
                                        )
                                    )
                                else:
                                    save_content_s3(
                                        s3_client,
                                        args.bucket_name,
                                        f'{args.bucket_prefix}/{arquivo}',
                                        response.content
                                    )
                            else:
                                logging.warning('response com status code: %s %s', response.status_code, response)
                        else:
                            logging.warning('response None "%s"', response)
                else:
                    logging.warning('Erro ao obter lista_para_download:%s, CNPJ:%s', (lista_para_download is not None) and lista_para_download['data'] and len(lista_para_download['data']) > 0, str(cnpj))

        else:
            logging.error('Lista de fundos para importar vazia.')
            return -1

        if countException > 0:
            logging.error(f'Ocorreram Exceptions durante a execucao do programa:{str(countException)}')
            return -2
            
        return 0  # ok, pelo menos alguma coisa funcionou
    else:
        logging.error('Argumentos invalidos')
        return -1  # Erro


if __name__ == '__main__':
    stage = os.getenv('stage', 'oficial')
    exit_code = 0
    print(f'Inicializando processo {datetime.datetime.now().isoformat()}')  # sem log configurado
    try:
        configureLogging()
    except Exception:
        print(f'Error configurando logging. não é possivel executar o processo')
        exit_code = -1

    if (exit_code == 0):
        args = None
        try:
            args = parseargs()

            exit_code = download(args, stage)
            logging.info('finalizando processo. exit_code: "%s"', exit_code)
        except Exception as xcp:
            logging.exception('except durante o download do arquivo "%s"', xcp)
            exit_code = -1

    sys.exit(exit_code)
