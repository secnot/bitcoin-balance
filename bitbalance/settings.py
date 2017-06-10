
Settings = {
    # 
    'MAX_BACKTRACK_BLOCKS': 100,

    # Time between sucessive poll request while waiting
    # for the next block
    'BITCOIND_POLL_PERIOD': 3,

    # Time between sucessive reconnet tries (in seconds)
    'BITCOIND_RECONNECT_PERIOD': 5,

    # Default bitcoind url
    'BITCOIND_URL': 'http://user:pass@localhost:8332',

    # Chain used by bitcoind 'testnet' or 'mainnet'
    'BITCOIN_CHAIN': 'testnet',

    # Faster blockchain synchronization at the expense of worse
    # responsiveness until the balance is up to date. Recommended
    # during first sync.
    'FAST_SYNC': True,

    # Size of in-memory address->balance cache
    'BALANCE_CACHE_SIZE': 500000,
}


