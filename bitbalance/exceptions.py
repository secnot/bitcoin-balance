class ChainError(Exception):
    """The block is not in the current block chain"""
    def __init__(self, message):
        self.message = message
