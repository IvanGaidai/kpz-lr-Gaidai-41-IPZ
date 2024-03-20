from datetime import datetime, timedelta
from binance.client import Client
import pandas as pd


def get_rsi(asset, periods):
  today = datetime.now().strftime('%Y-%m-%d')
  yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

  klines = Client().get_historical_klines(
    symbol=asset,
    interval=Client.KLINE_INTERVAL_1MINUTE,
    start_str=yesterday,
    end_str=today
  )

  df = pd.DataFrame(klines)[[0, 1, 2, 3, 4, 5]]

  df[0] = df[0].apply(lambda x: datetime.fromtimestamp(x / 1000))

  for col in df.columns[1:]:
    df[col] = pd.to_numeric(df[col])


  df = df.rename(columns={
    0: 'time', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'
  })

  price_diff = df['close'].diff(1)
  gains = price_diff.where(price_diff > 0, 1)
  losses = price_diff.where(price_diff < 0, -1).abs()

  result = pd.DataFrame({'time': df['time']})
  for period in periods:

    avg_gain = gains.rolling(window=period + 1, min_periods=1).mean()
    avg_loss = losses.rolling(window=period + 1, min_periods=1).mean()


    rs_first = avg_gain.iloc[:period] / avg_loss.iloc[:period]
    rsi_first = 100 - (100 / (1 + rs_first))
    rs_rest = avg_gain[period:] / avg_loss[period:]  
    rsi_rest = 100 - (100 / (1 + rs_rest))

    result['RSI' + str(period)] = pd.concat([pd.Series(rsi_first), rsi_rest])

  return result



print(get_rsi("BTCUSDT", [14, 27, 100]))