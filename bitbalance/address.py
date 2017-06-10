from bitcoin.base58 import CBase58Data

# Supported address version bytes

MAIN
BITCOIN_VERSION_BYTES = set([
        111, # Testnet pubkey hash
        196, # Testnet script hash
        0,   # MainNet pubkey hash
        5])   # MainNet script hash


def is_valid_bitcoin_address(address, testnet=True):
    """
    Check the address is a valide P2SH or P2PK bitcoin address
    
    Arguments:
        address(str)
        testnet(bool): True to also accept testnet addresses

    Returns:
        (bool): True if it is a valid, False otherwise
    """
    try:
        assert isinstance(address, str)
        assert 25 < len(address) < 36
        addr = CBase58Data(address)
        assert addr.nVersion in BITCOIN_VERSION_BYTES
        return True
    except Exception:
        return False

