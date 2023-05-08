class Setting:
    is_back_test: bool = False

    low_risk_per_trade = 0.02
    high_risk_per_trade = 0.1

    taker_fee = 0.0004
    maker_fee = 0.0002

    paper_leverage = 100
    stop_loss = 0.005  # 1%
    take_profit = 0.015  # 1.5%
    trailing_stop_loss = 0.01  # 0.5%
    trailing_take_profit = 0.01  # 0.5%
    max_trailing_takes = 2

    ema1_length = 9
    ema1_amplitude = 2

    ema2_length = 20
    ema2_amplitude = 2.5

    ema3_length = 50
    ema3_amplitude = 2.5

    touches = 0
    wins = 0
    loses = 0
    trailing_loses = 0

    last_action = None
    last_orders = []

    OPEN_LONG = "open_long"
    OPEN_SHORT = "open_short"
    INCREASE_LONG = "increase_long"
    INCREASE_SHORT = "increase_short"

    DIRECTION_LONG = "Long"
    DIRECTION_SHORT = "Short"

    symbols_settings = {}

    symbols = [
        'APTUSDT', 'DYDXUSDT', 'ANKRUSDT',
        'OPUSDT', 'MATICUSDT', 'DOTUSDT',
        'APEUSDT', 'AVAXUSDT', '1000SHIBUSDT',
        'IMXUSDT', 'LINKUSDT', 'GALAUSDT',
        'BNBUSDT', 'INJUSDT', 'FILUSDT',
        'SOLUSDT', 'FLMUSDT', 'FTMUSDT',
        'ETCUSDT', 'TRXUSDT', 'LTCUSDT',
        'MANAUSDT', 'LDOUSDT', 'XRPUSDT',
        'ADAUSDT', 'ATOMUSDT', 'DOGEUSDT'

        # 'AAVEUSDT', 'NEARUSDT',
    ]

    def __init__(self):
        pass
