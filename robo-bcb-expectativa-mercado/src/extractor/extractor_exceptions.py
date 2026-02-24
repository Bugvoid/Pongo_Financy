class UnknownExchange(Exception):
    """Raised when exchange were not found"""
    def __init__(self, exchange):
        super().__init__(f'Unknow exchange found: "{exchange}"')

class UnknownFile(Exception):
    """Raised when no extractor matches input file"""
    def __init__(self, filename):
        super().__init__(f'No extractor found for input file: "{filename}"')