import logging
import os
import json
import time
from datetime import datetime
from extractor_base import BaseExtractor
from extractor_exceptions import UnknownExchange, UnknownFile


class ExpQuotesExtractor(BaseExtractor):
    
    def __init__(self, args):
        super().__init__(args)
        self.list_expectation = []
        self.config = self.request_list('config')
        self.count_error = 0
    
    @classmethod
    def name(self):
        return 'ExpQuotes'
    
    def get_value(self, exp, field):
        try:
            value = eval(exp['config_type'][field].format(**exp)) 
        except:
            value = '-'
        
        return value
       
    def parse(self, file_path):
        self.list_expectation = []
        with open(file_path, 'r', encoding='utf-8') as stream:
            obj_json = self.key_to_lowercase(json.load(stream))
            for reg in obj_json['value']:
                for exp in self.yield_exp(reg):
                    self.list_expectation.append(exp)
        
    def export(self, file):
        if self.list_expectation:
            self.filename = os.path.splitext(file)[0]