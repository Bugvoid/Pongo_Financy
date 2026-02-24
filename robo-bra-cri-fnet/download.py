#!/usr/bin/env python
# -*- coding: utf-8 -*-

from email.policy import default
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
from logging.handlers import RotatingFileHandler

'''
# suprimir mensagens de que nao usa certificado nas requisicoes https
InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised.
See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
'''
urllib3.disable_warnings()

PAGINA_INICIO_COOKIE_TOKEN = 'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCertificadosCVM#'
URL_DOWNLOAD_XML = 'https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento'  # id = da emp. Ex: 76428 & CVM=true
URL_PESQUSA_GERENCIADOR_DOCUMENTOS = 'https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados'  # página do grid inicial
# https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?d=5&s=0&l=10&o[0][dataEntrega]=desc&tipoFundo=5&idCategoriaDocumento=6&idTipoDocumento=86&idEspecieDocumento=0&cnpj=BRBZRSCRI0C9&dataInicial=01/01/2000&dataFinal=27/01/2020&paginaCertificados=true&_=1649685983701

MAX_RESULT_API = 200
data_de_cache = None
data_ate_cache = None

class CallCounted:
    """Decorator to determine number of calls for a method"""

    def __init__(self,method):
        self.method=method
        self.counter=0

    def __call__(self,*args,**kwargs):
        self.counter+=1
        return self.method(*args,**kwargs)

def configureLogging():
    loglevel = logging.DEBUG if (("-v" in sys.argv) or ("--verbose" in sys.argv)) else logging.INFO #mostrar log debug
    logging.basicConfig(
        level=loglevel,
        format='%(asctime)s %(name)s %(levelname)s - %(message)s',
        handlers=[
            RotatingFileHandler(__file__.replace('.py', '.log'), maxBytes=1000000, backupCount=9),
            logging.StreamHandler(sys.stdout)])

    logging.error = CallCounted(logging.error)
    logging.critical = CallCounted(logging.critical)
    logging.exception = CallCounted(logging.exception)
    logging.info('args: "%s"', sys.argv)


def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def create_parser():
    parse = argparse.ArgumentParser()

    parse.add_argument(
        "-prefix", "--file_prefix",  # Tipo do fundo
        required=True,
        type=str,
        help='Define prefixo do arquivo a ser salvo no download. Ex: CRI = CRI-BRHBSCCRI0O3-20200131-20200128-114809-110294.xml'
    )

    parse.add_argument(
        "-mdsd", "--max_dias_sem_download",
        type=int,
        default=3,
        help='quantidade de dias que será considerada para o proceso gerar erro (ou não) quando a lista de download estiver vazia'
    )

    parse.add_argument(
        "-l", "--limit_download",
        type=int,
        default=100,
        help='quantidade maxima de downloads por execução (0 = sem limites)'
    )
    
    parse.add_argument(
        "-id_tipo", "--id_tipo_fundo",  # TIPO_FUNDO
        required=True,
        type=int,
        default=5,
        help='Id do tipo de certificado. 5=CRI'
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
        default=86,
        help='Id do tipo documento do campo em "extrair inf. mensal" do fnet. 86=informe mensal de CRI (Anexo 32, II ICVM 480)'
    )

    parse.add_argument(
        "-id_esp", "--id_especie_doc",  # ID_ESPECIE_DOCUMENTO
        required=False,
        type=int,
        default=0,  # 0=todos
        help='Id da espécie documento do campo em "extrair inf. mensal" do fnet. 0=todos'
    )

    parse.add_argument(
        "-ident", "--identificador",  # exemplo: BRHBSCCRI0O3
        required=False,
        help='Código de Identificação do Certificado (Código ISIN ou CETIP): Se informado ignora lista do arquivo.'        
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
        "-f", "--force",
        action='store_true',
        default=False,
        help='Força o download de informes já baixados anteriormente'
    )

    parse.add_argument(
        "-v", "--verbose",
        action="store_true",
        default=False,
        help="flag para mudar o nivel do logging para debug"
    )

    return parse


def _get_data_de(args):

    global data_de_cache
    if data_de_cache is None:

        '''
        data incial: caso nao informada assume o dia do ultimo download do robo, caso seja 
        a primeira execução, exigirá uma data.
        '''
        data_de = args.data_de
        try:
            if not data_de:
                logging.error('Nenhuma data de processamento anterior encontrada, deverá ser informado uma data inicial ou um range de datas')
                return False, None 
                
            data_de_cache = data_de
            return True, data_de
        
        except ValueError:
            logging.error('parameter data_de with value invalid: "%s"', args.data_de if args.data_de else data_de)
            return False, None
        
    else:
        return True, data_de_cache


def _get_data_ate(args):

    global data_ate_cache
    if data_ate_cache is None:

        try:
            ok, data_de = _get_data_de(args)

            if args.data_ate:
                data_ate = args.data_ate

                if data_de and data_ate < data_de:
                    return False, None
            else:
                # se data_de foi calculado vamos até somar alguns dias
                if data_de+datetime.timedelta(180) > datetime.datetime.today():
                    data_ate = datetime.datetime.today()
                else:
                    data_ate = data_de+datetime.timedelta(180)
                logging.info(f'assumindo "data_ate" a partir de {data_ate}')

            data_ate_cache = data_ate
            return True, data_ate
        except ValueError:
            logging.error('parameter data_ate with value invalid: "%s"', args.data_ate)
            return False, None

    else:
        return True, data_ate_cache


def validate_args(args):

    ok, data = _get_data_de(args)
    if not ok:
        return False  # Erro

    ok, data = _get_data_ate(args)
    if not ok:
        return False  # Erro

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


def nome_arquivo(tipo_fundo, identificador, data_ref, data_pub, extensao):
    if extensao not in ['pdf', 'xml']:
        extensao = 'xml'
    '''
        # formato original: <carga format-output-fileName="CRI-%1$tY%1$tm%1$td-%1$tk%1$tM%1$tS.cri">
        # Atual:
        # Formato: CRI-BRRGSSCRI043-20200131-20200128-114809-110294.xml
        # Formato: Tipo-identificador-YYYYMMDD-YYYYMMDD-HMS
    '''
    return '-'.join([
        tipo_fundo,
        identificador,
        data_ref.strftime('%Y%m%d'),
        data_pub.strftime('%Y%m%d-%H%M%S')
    ]) + '.' + extensao

def _make_params_request(args, identificador, start=0, limit=MAX_RESULT_API):

    ok, _de = _get_data_de(args)
    ok, _ate = _get_data_ate(args)

    _de = _de.strftime('%d/%m/%Y') if _de is not None else None
    _ate = _ate.strftime('%d/%m/%Y') if _ate is not None else None
    
    params = {
        'd': '0',
        's': str(start),
        'l': str(limit),
        'o[0][dataEntrega]': 'asc',
        'tipoFundo': str(args.id_tipo_fundo),
        'cnpj': identificador, #Na pagina de busca o campo identificador corresponde ao codigoisin ou cetip
        'idCategoriaDocumento': str(args.id_categoria_doc),
        'idTipoDocumento': str(args.id_tipo_doc),
        'idEspecieDocumento': str(args.id_especie_doc),
        'situacao': 'A',  # somente os documentos ATIVOs
        'dataInicial': _de,
        'dataFinal': _ate,
        'paginaCertificados': 'true',
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


def _make_api_headers(last_response, identificador):

    if identificador:
        referer = 'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCertificadosCVM?cnpj=' + identificador
    else:
        referer = 'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCertificadosCVM'

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Host': 'fnet.bmfbovespa.com.br',
        'Pragma': 'no-cache',
        'Referer': referer,
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
        'Referer': 'https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCertificadosCVM',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Upgrade-Insecure-Requests': '1'
    }

    if "CSRFToken" in last_response.headers:
        headers["CSRFToken"] = last_response.headers["CSRFToken"]

    if "Cookie" in last_response.headers:
        headers["Cookie"] = last_response.headers["Cookie"]

    return headers


def _get_session():
    session = requests.Session()
    first_headers = _make_headers_first_request()
    resp = session.get(PAGINA_INICIO_COOKIE_TOKEN, verify=False, headers=first_headers)

    if resp.status_code == 200:
        return session, resp
    else:
        return None, None


def get_json_download(args, identificador):
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
            json_request = _make_params_request(args, identificador, start)
            req_header = _make_api_headers(resp, identificador)
            resp_json = session.get(
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
                    logging.warning(f'Pesquisa não retornou dados para o filtro: args {args} e identificador {identificador}')
                    
                if msg:
                    continua = False
                    logging.warning(f'Pagina retornou a mensagem {msg} para o filtro: args {args} e identificador {identificador}')
                    

                if not data:
                    continua = False
                    logging.warning(f'Pagina não retornou dados para o filtro: {args} e identificador {identificador}')
                    

                # https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosCertificadosCVM?d=0&s=2252&l=200&o[0][dataEntrega]=desc&tipoFundo=5&idCategoriaDocumento=6&idTipoDocumento=86&idEspecieDocumento=0&dataInicial=01/01/2000&dataFinal=27/01/2020&_=1580150474305

                # concatena todas as listagens
                if continua:
                    if data and len(data) > 0:
                        result['data'].extend(data)
            else:
                continua = False
                logging.error(f'Erro ao pegar json da paginação atual. request: {json_request}, args: {args} e identificador {identificador}')
                
            if continua:
                if recordsTotal > MAX_RESULT_API:
                    if start < recordsTotal:
                        start = (start + MAX_RESULT_API)
                else:
                    logging.info(f'Tentou-se extrair  {recordsTotal} itens para download.')
                    continua = False
    else:
        logging.error('Erro ao iniciar pagina da sessao e cookies.')

    if not result['data'] and len(result['data']) > 0:
        logging.warning(f'Pesquisa não encontrou dados para argumentos: {args} , identificador {identificador}, start {start}.')
    else:
        logging.info(f'classificando retorno da lista de API por data')
        sorted(result['data'], key=lambda item: item['dataEntrega'])

    return result


def save_content_s3(s3_client, bucket, key, bcontent):

    temp_file = tempfile.TemporaryFile()

    try:
        temp_file.write(bcontent)
        temp_file.seek(0)

        s3_client.upload_fileobj(temp_file, bucket, key, ExtraArgs={'ACL':'bucket-owner-full-control'})
        logging.info(f'Arquivo gravado s3:{bucket}/{key}')
    except Exception as xcp:
        logging.exception('except ao salvar o arquivo no bucket s3 "%s"', xcp)
    finally:
        temp_file.close()

    return True


def download(args):
    num_download = 0
    if validate_args(args):
        s3_client = boto3.client('s3')
        
        identificador = args.identificador if args.identificador else None

        lista_para_download = get_json_download(args, identificador)

        if lista_para_download['data'] and len(lista_para_download['data']) > 0:
            session, resp = _get_session()
            logging.info(f"lista = {len(lista_para_download['data'])} arquivos para analisar se vai baixar")
            for download_item in lista_para_download['data']:

                if (args.limit_download != 0) and (num_download >= args.limit_download):
                    logging.warning(f'numero maximo de download: {num_download}/{args.limit_download} execute novamente para continuar')
                    break

                if not args.identificador:
                    descricao_fundo = download_item['descricaoFundo']
                    identificador = descricao_fundo.split(' ')[-1]

                data_pub = datetime.datetime.strptime(download_item['dataEntrega'], '%d/%m/%Y %H:%M')

                req_header = _make_download_headers(resp)
                response = session.get(
                    URL_DOWNLOAD_XML,
                    params={'id': download_item['id']},
                    verify=False,
                    headers=req_header
                )

                if not(response is None):
                    if (response.status_code == 200):
                        extensao = 'xml'
                        if "Content-Disposition" in response.headers:
                            # 'Content-Disposition']: attachment; filename="16915840000114-IFP11012017V01-000008070.pdf"
                            filename = response.headers['Content-Disposition']
                            extensao = filename[-4:-1]

                        if(len(download_item['dataReferencia']) > 7):
                            data_ref = datetime.datetime.strptime(download_item['dataReferencia'].split(' ')[0], '%d/%m/%Y')
                        
                        arquivo = nome_arquivo(
                            args.file_prefix,
                            identificador,
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

                        num_download = num_download + 1

                    else:
                        logging.warning('response com status code: %s %s', response.status_code, response)
                else:
                    logging.warning('response None "%s"', response)

        return 0  # ok, pelo menos alguma coisa funcionou
    else:
        logging.error('32069 - Argumentos invalidos')
        return -1  # Erro


if __name__ == '__main__':
    stage = os.getenv('stage', 'oficial')
    exit_code = 0
    print(f'Inicializando processo {datetime.datetime.now().isoformat()}')  # sem log configurado
    try:
        configureLogging()
    except Exception:
        print(f'32070 - Error configurando logging. não é possivel executar o processo')
        exit_code = -1

    if (exit_code == 0):
        args = None
        try:
            args = parseargs()
            exit_code = download(args)
            logging.info('finalizando processo. exit_code: "%s"', exit_code)
        except Exception as xcp:
            logging.exception('32071 - except durante o download do arquivo "%s"', xcp)
            exit_code = -1

    if exit_code == 0:
        exit_code = logging.error.counter
        if exit_code != 0:
            logging.info(f'exit_code = {exit_code} error count (numero de erros)')
    if exit_code == 0:
        exit_code = logging.critical.counter
        if exit_code != 0:
            logging.info(f'exit_code = {exit_code} critical count (numero de erros criticos)')
    if exit_code == 0:
        exit_code = logging.exception.counter
        if exit_code != 0:
            logging.info(f'exit_code = {exit_code} exception count (numero de exceptions)')

    sys.exit(exit_code)
