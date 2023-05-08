class Account:
    balance = 0
    symbol_position = None
    long_position = False
    short_position = False
    entry_price = 0
    stop_loss_price = 0
    take_profit_price = 0
    asset_size = 0
    position_size = 0
    position_fee = 0

    last_stop_loss_order_id = 0
    last_take_profit_order_id = 0

    def __init__(self, balance: float = 0):
        self.balance = balance
        pass
