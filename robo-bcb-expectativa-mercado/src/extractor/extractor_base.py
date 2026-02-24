import abc
import logging
import unicodedata
import re
from robo_api_lib import RoboApiLib

class BaseExtractor(abc.ABC):
    
    def __init__(self, args):
        super().__init__()
        self.output_dir=args.output_dir
        self.input_dir=args.input_dir
        self.bkp_dir=args.bkp_dir
        self.api_base=args.api_base
        self.api_version=args.api_version
        self.api_region=args.api_region
        self.api_stage=args.api_stage
        self.api_timeout_secs=args.api_timeout_secs
        self.max_linhas=args.max_linhas
        self.limit_files=args.limit_files
        self.first_job=args.first 

        logging.info(f'Instantiated "{self.name()}" extractor.')
        
    @classmethod
    @abc.abstractmethod
    def name(self) -> str:
        """ Returns the extractor name """
        raise NotImplementedError
    
    @abc.abstractmethod
    def parse(self, file_path):
        """ This method must not be called directly, call do_parse """
        raise NotImplementedError
    
    def do_parse(self, filename):
        try:
            logging.info(f'Extractor "{self.name()}" parsing {filename}.')
            self.parse(filename)
            logging.info(f'Succefull extract: {filename} with extractor: {self.name()}')
        except:
            logging.info(f'An error occurred while extracting: {filename} with extractor: {self.name()}')
            raise
    
    @abc.abstractmethod
    def export(self):
        """ Must be called after all files in the input directory have been parsed"""
        raise NotImplementedError

    def key_to_lowercase(self, obj_json):
        if isinstance(obj_json, dict):
            return {key.lower(): self.key_to_lowercase(value) for key, value in obj_json.items()}
        elif isinstance(obj_json, list):
            return [self.key_to_lowercase(item) for item in obj_json]
        else:
            return obj_json

    def request_list(self, list_name, robo_name='robo-bcb-expectativa-mercado'):
        """ Request list from api """
        try:
            logging.info(f'Loading list: "{list_name}" from: {robo_name}')
            robo_api = RoboApiLib(
                robo=robo_name,
                api_base=self.api_base,
                version=self.api_version,
                region=self.api_region,
                stage=self.api_stage
            )
            return robo_api.get_json_content(
                list_name=list_name,
                wait_timeout=self.api_timeout_secs
            )
        except:
            logging.exception(f'Error while requesting list: "{list_name}" from: {robo_name}')
            raise
    
    def clean_characters(self, text):
        text = unicodedata.normalize('NFD', text)
        text = text.encode('ascii','ignore').decode('utf-8')
        return re.sub(r'[^0-9A-Za-z]','', text).upper()
    
    def yield_exp(self, reg):
        found = False
        for exp in self.config['types']:
            if exp['exp_name'] == reg['indicador']:
                found = True
                yield {**reg, 'config_type':exp}

        if not found:
            logging.warning(f'Unknown Expectation Type found: "{reg["indicador"]}"')