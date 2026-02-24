import argparse
import logging
import os
import sys
import shutil
from time import sleep
from extractor_exceptions import UnknownFile
from datetime import datetime

from extractor_base import BaseExtractor
from extractor_expectation_quotes import ExpQuotesExtractor

def configureLogging():
    logFormatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.INFO)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)

    logging.info('inicializando processo "%s"', datetime.now().isoformat())
    logging.info('python "%s"', sys.version)
    logging.info('args: "%s"', sys.argv)
    
def parse_command_line():
    parse = argparse.ArgumentParser()
    parse.add_argument(
        "-i", "--input_dir",
        required=True,
        help='pasta de entrada para extrair informações e gerar os comandos ecoinput'
    )
    parse.add_argument(
        "-o", "--output_dir",
        required=True,
        help='pasta para gravar o arquivo que será importado pelo ecoinput'
    )
    parse.add_argument(
        "-d", "--bkp_dir",
        required=False,
        help='pasta de destino dos arquivos presentes na pasta de input em caso de sucesso na geração do arquivo ecoinput'
    )
    parse.add_argument(
        "-e", "--error_dir",
        required=False,
        help='pasta de destino dos arquivos presentes na pasta de input em caso de erro na geração do arquivo ecoinput'
    )
    parse.add_argument(
        '-as',
        '--api_stage',
        required=True,
        help='API STAGE',
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
        '-apir',
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
        '-limit',
        '--limit_files',
        metavar='LIMIT-FILES',
        default=0,
        type=int,
        help='quantidade máxima de arquivos por execução. (0=sem limite)',
    )
    parse.add_argument(
        '-max',
        '--max_linhas',
        metavar='max',
        type=int,
        help='Maximo numero de linhas por arquivo de importacao'
    )
    parse.add_argument(
        '-f',
        '--first',
        metavar='first',
        default=False,
        type=valid_boolean,
        help='Primeiro processamento do robo'
    )
    
    return parse.parse_args()

def valid_boolean(s):
    try:
        return True if s == 'true' else False 
    except ValueError:
        msg = "Parâmetro de boolean inválida!: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def validate_args(args):
    if not args.input_dir or not os.path.exists(args.input_dir):
        raise Exception(f'Invalid input dir: "{args.input_dir}"')
    
    if not args.output_dir or not os.path.exists(args.output_dir):
        raise Exception(f'Invalid output dir: "{args.output_dir}"')
    
    if not args.error_dir or not os.path.exists(args.error_dir):
        raise Exception(f'Invalid error dir: "{args.error_dir}"')

    if not args.bkp_dir or not os.path.exists(args.bkp_dir):
        raise Exception(f'Invalid historico dir: "{args.bkp_dir}"')

def setup_extractors(args):
    args.extractors = []
    for extractor in BaseExtractor.__subclasses__():
        args.extractors.append(extractor(args))

def fn_move(src, dest, retry=3):
    attempt = 0
    while attempt < retry:
        try:
            shutil.move(src, dest)
            break
        except:
            logging.exception(f'An error occurred while moving file: "{src}"')
            attempt += 1
            sleep(2)
            if attempt == retry:
                raise

def run(args):
    qtd_files = 0
    error = 0
    for file in sorted(os.listdir(args.input_dir))[:args.limit_files]:
        for extractor in args.extractors:    
            try:
                logging.info(f'Extractor: "{extractor.name()}" to inputfile: "{file}"')
                extractor.parse(os.path.join(args.input_dir, file))
                extractor.export(file)
                error += extractor.count_error
            except:
                error += 1
    
        if error > 0:
            logging.exception(f'An error occurred while extracting: "{file}"')
            fn_move(os.path.join(args.input_dir, file), os.path.join(args.error_dir, file))                        
        else:
            fn_move(os.path.join(args.input_dir, file), os.path.join(args.bkp_dir, file))
        qtd_files += 1
    
    return error

if __name__ == '__main__':
    exit_code = 0
    try:
        configureLogging()
        args = parse_command_line()
        validate_args(args)
        setup_extractors(args)
        exit_code = run(args)
    except Exception as Ex:
        logging.exception(f'An unexpected exception was raised: {Ex}')
        exit_code = -1
    
    sys.exit(exit_code)