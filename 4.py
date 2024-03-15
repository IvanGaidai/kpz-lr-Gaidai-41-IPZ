import pandas as pd
import ta
from matplotlib import pyplot as plt
from binance import Client

# Загрузка 
k_lines = Client().get_historical_klines(
    symbol="BTCUSDT",
    interval=Client.KLINE_INTERVAL_1MINUTE,
    start_str="1 day ago UTC",
    end_str="now UTC"
)

# Створення DataFrame
k_lines = pd.DataFrame(k_lines, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
k_lines['time'] = pd.to_datetime(k_lines['time'], unit='ms')
k_lines['close'] = k_lines['close'].astype(float)
k_lines['high'] = k_lines['high'].astype(float)
k_lines['low'] = k_lines['low'].astype(float)
k_lines['open'] = k_lines['open'].astype(float)

# Роз індикат
periods = [14, 27, 100]
for period in periods:
    rsi_indicator = ta.momentum.RSIIndicator(k_lines['close'], period)
    k_lines[f'RSI_{period}'] = rsi_indicator.rsi()

# Віз та індикат
plt.figure(figsize=(14, 7))
plt.subplot(6, 1, 1)
plt.plot(k_lines['time'], k_lines['close'], label='Close Price')
plt.title('Close Price')

plt.subplot(6, 1, 2)
plt.plot(k_lines['time'], k_lines['RSI_14'], label='RSI_14', color='purple')
plt.title('RSI_14')

plt.subplot(6, 1, 3)
plt.plot(k_lines['time'], k_lines['RSI_27'], label='RSI_27', color='purple')
plt.title('RSI_27')

plt.subplot(6, 1, 4)
plt.plot(k_lines['time'], k_lines['RSI_100'], label='RSI_100', color='purple')
plt.title('RSI_100')

plt.tight_layout()
plt.show()