class BitcoinError(Exception):
    
    def __init__(self, message):
        self.message = message


class BacktrackError(Exception):
    """Unable to backtrack"""
    pass

class ChainError(Exception):
    """The block is not in the current block chain"""
    pass
