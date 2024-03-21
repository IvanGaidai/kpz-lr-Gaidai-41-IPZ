import pandas as pd
import ta
from matplotlib import pyplot as plt
from binance import Client

# Загрузка данных
k_lines = Client().get_historical_klines(
    symbol="BTCUSDT",
    interval=Client.KLINE_INTERVAL_1MINUTE,
    start_str="1 day ago UTC",
    end_str="now UTC"
)

# Создание DataFrame
k_lines = pd.DataFrame(k_lines, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
k_lines['time'] = pd.to_datetime(k_lines['time'], unit='ms')
k_lines['close'] = k_lines['close'].astype(float)
k_lines['high'] = k_lines['high'].astype(float)
k_lines['low'] = k_lines['low'].astype(float)
k_lines['open'] = k_lines['open'].astype(float)

# Расчет индикаторов
k_lines['RSI'] = ta.momentum.RSIIndicator(k_lines['close']).rsi()
k_lines['CCI'] = ta.trend.CCIIndicator(k_lines['high'], k_lines['low'], k_lines['close']).cci()
k_lines['MACD'] = ta.trend.MACD(k_lines['close']).macd()
k_lines['ATR'] = ta.volatility.AverageTrueRange(k_lines['high'], k_lines['low'], k_lines['close']).average_true_range()
k_lines['ADX'] = ta.trend.ADXIndicator(k_lines['high'], k_lines['low'], k_lines['close']).adx()

# Создание столбцов для сигналов
k_lines['RSI_buy_signal'] = (k_lines['RSI'] < 30) & (k_lines['RSI'].shift() >= 30)
k_lines['RSI_sell_signal'] = (k_lines['RSI'] > 70) & (k_lines['RSI'].shift() <= 70)
k_lines['CCI_buy_signal'] = (k_lines['CCI'] < -100) & (k_lines['CCI'].shift() >= -100)
k_lines['CCI_sell_signal'] = (k_lines['CCI'] > 100) & (k_lines['CCI'].shift() <= 100)
k_lines['MACD_buy_signal'] = (k_lines['MACD'].shift() < 0) & (k_lines['MACD'] > 0)
k_lines['MACD_sell_signal'] = (k_lines['MACD'].shift() > 0) & (k_lines['MACD'] < 0)
k_lines['ATR_buy_signal'] = k_lines['ATR'] > k_lines['ATR'].shift()
k_lines['ATR_sell_signal'] = k_lines['ATR'] < k_lines['ATR'].shift()
k_lines['ADX_buy_signal'] = (k_lines['ADX'] > 25) & (k_lines['ADX'].shift() <= 25)
k_lines['ADX_sell_signal'] = (k_lines['ADX'] < 25) & (k_lines['ADX'].shift() >= 25)

# Визуализация закрытия и индикаторов с сигналами
plt.figure(figsize=(14, 7))
plt.subplot(6, 1, 1)
plt.plot(k_lines['time'], k_lines['close'], label='Close Price')
plt.title('Close Price')

plt.subplot(6, 1, 1)
plt.plot(recent_data['time'], recent_data['close'], label='Close Price')
plt.plot(recent_data['time'], recent_data['SMA_5'], label='SMA (5)')
plt.plot(recent_data['time'], recent_data['SMA_20'], label='SMA (20)')
plt.title('Close Price')
plt.legend()

# Subplot for RSI with signals
plt.subplot(6, 1, 2)
plt.plot(recent_data['time'], recent_data['RSI'], label='RSI', color='purple')
plt.scatter(recent_data.loc[recent_data['RSI_buy_signal'], 'time'], recent_data.loc[recent_data['RSI_buy_signal'], 'RSI'], marker='^', color='green', label='Buy Signal')
plt.scatter(recent_data.loc[recent_data['RSI_sell_signal'], 'time'], recent_data.loc[recent_data['RSI_sell_signal'], 'RSI'], marker='v', color='red', label='Sell Signal')
plt.title('RSI')
plt.legend()

# Subplots for other indicators with signals (modify labels as needed)
plt.subplot(6, 1, 3)
plt.plot(recent_data['time'], recent_data['MACD'], label='MACD', color='green')
plt.scatter(recent_data.loc[recent_data['MACD_buy_signal'], 'time'], recent_data.loc[recent_data['MACD_buy_signal'], 'MACD'], marker='^', color='green', label='Buy Signal')
plt.scatter(recent_data.loc[recent_data['MACD_sell_signal'], 'time'], recent_data.loc[recent_data['MACD_sell_signal'], 'MACD'], marker='v', color='red', label='Sell Signal')
plt.title('MACD')  
plt.legend()

plt.subplot(6, 1, 4)
plt.plot(recent_data['time'], recent_data['ATR'], label='ATR', color='black')
plt.scatter(recent_data.loc[recent_data['ATR_buy_signal'], 'time'], recent_data.loc[recent_data['ATR_buy_signal'], 'ATR'], marker='^', color='green', label='Buy Signal')
plt.scatter(recent_data.loc[recent_data['ATR_sell_signal'], 'time'], recent_data.loc[recent_data['ATR_sell_signal'], 'ATR'], marker='v', color='red', label='Sell Signal')
plt.title('ATR')  
plt.legend()

plt.subplot(6, 1, 5)
plt.plot(recent_data['time'], recent_data['ADX'], label='ADX', color='black')
plt.scatter(recent_data.loc[recent_data['ADX_buy_signal'], 'time'], recent_data.loc[recent_data['ADX_buy_signal'], 'ADX'], marker='^', color='green', label='Buy Signal')
plt.scatter(recent_data.loc[recent_data['ADX_sell_signal'], 'time'], recent_data.loc[recent_data['ADX_sell_signal'], 'ADX'], marker='v', color='red', label='Sell Signal')
plt.title('ADX')  
plt.legend()

plt.subplot(6, 1, 6)
plt.plot(recent_data['time'], recent_data['CCI'], label='CCI', color='black')
plt.scatter(recent_data.loc[recent_data['CCI_buy_signal'], 'time'], recent_data.loc[recent_data['CCI_buy_signal'], 'CCI'], marker='^', color='green', label='Buy Signal')
plt.scatter(recent_data.loc[recent_data['CCI_sell_signal'], 'time'], recent_data.loc[recent_data['CCI_sell_signal'], 'CCI'], marker='v', color='red', label='Sell Signal')
plt.title('CCI')  
plt.legend()

plt.tight_layout()
plt.show()
