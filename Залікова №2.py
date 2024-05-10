import pandas as pd
import ta
from binance.client import Client
from dataclasses import dataclass
from typing import List

@dataclass
class Signal:
    time: pd.Timestamp
    asset: str
    quantity: float
    side: str
    entry: float
    take_profit: float
    stop_loss: float
    result: float
    closed_by: str

def perform_backtesting(k_lines: pd.DataFrame):
    signals = create_signals(k_lines)
    results = []
    for signal in signals:
        start_index = k_lines[k_lines['time'] == signal.time].index[0]
        data_slice = k_lines.iloc[start_index:]
        for candle_id in range(len(data_slice)):
            if (signal.side == "sell" and data_slice.iloc[candle_id]["low"] <= signal.take_profit) or (signal.side == "buy" and data_slice.iloc[candle_id]["high"] >= signal.take_profit):
                signal.result = signal.take_profit - signal.entry if signal.side == 'buy' else (signal.entry - signal.take_profit)
            elif (signal.side == "sell" and data_slice.iloc[candle_id]["high"] >= signal.stop_loss) or (signal.side == "buy" and data_slice.iloc[candle_id]["low"] <= signal.stop_loss):
                signal.result = signal.stop_loss - signal.entry if signal.side == 'buy' else (signal.entry - signal.stop_loss)
            if signal.result is not None:
                signal.closed_by = "TP" if signal.result > 0 else "SL"
                results.append(signal)
                break
    return results

def calculate_pnl(trade_list: List[Signal]):
    total_pnl = 0
    for trade in trade_list:
        total_pnl += trade.result
    return total_pnl

def calculate_statistics(trade_list: List[Signal]):
    total_trades = len(trade_list)
    win_trades = sum(1 for trade in trade_list if trade.result > 0)
    win_rate = win_trades / total_trades if total_trades > 0 else 0
    profit_factor_val = profit_factor(trade_list)
    total_pnl = calculate_pnl(trade_list)

    print(f"Total PNL: {total_pnl}")
    print(f"Win Rate: {win_rate * 100}%")
    print(f"Profit Factor: {profit_factor_val}")

    if total_pnl > 0.5 and win_rate > 0.4 and profit_factor_val > 1.3:
        print("Strategy meets profitability criteria")
    else:
        print("Strategy does not meet profitability criteria")

def profit_factor(trade_list: List[Signal]):
    total_loss = 0
    total_profit = 0
    for trade in trade_list:
        if trade.result > 0:
            total_profit += trade.result
        else:
            total_loss += trade.result
    return total_profit / abs(total_loss) if abs(total_loss) > 0 else float('inf')

def create_signals(k_lines):
    signals = []
    for i in range(len(k_lines)):
        current_price = k_lines.iloc[i]['close']
        if (k_lines.iloc[i]['rsi'] < 30 and
            k_lines.iloc[i]['macd'] > k_lines.iloc[i]['macd_signal'] and
            k_lines.iloc[i]['ema'] < k_lines.iloc[i]['vwma'] and
            k_lines.iloc[i]['sma'] > k_lines.iloc[i]['ema'] and
            k_lines.iloc[i]['adx'] > 20):
            signal = 'buy'
        elif (k_lines.iloc[i]['rsi'] > 70 and
              k_lines.iloc[i]['macd'] < k_lines.iloc[i]['macd_signal'] and
              k_lines.iloc[i]['ema'] > k_lines.iloc[i]['vwma'] and
              k_lines.iloc[i]['sma'] < k_lines.iloc[i]['ema'] and
              k_lines.iloc[i]['adx'] > 20):
            signal = 'sell'
        else:
            continue

        if signal == "buy":
            stop_loss_price = round((1 - 0.0075) * current_price, 2)
            take_profit_price = round((1 + 0.0215) * current_price, 2)
        elif signal == "sell":
            stop_loss_price = round((1 + 0.0075) * current_price, 2)
            take_profit_price = round((1 - 0.0215) * current_price, 2)

        signals.append(Signal(
            k_lines.iloc[i]['time'],
            k_lines['symbol'][0],
            100,
            signal,
            current_price,
            take_profit_price,
            stop_loss_price,
            None, None
        ))
    return signals

client = Client()
symbols = ["BTCUSDT", "ETHUSDT", "ETHBTC", "BNBUSDT"]
for symbol in symbols:
    k_lines = client.get_historical_klines(
        symbol=symbol,
        interval=Client.KLINE_INTERVAL_1MINUTE,
        start_str="2 years ago UTC",
        end_str="now UTC"
    )

    # Creating DataFrame
    k_lines = pd.DataFrame(k_lines, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'])
    k_lines['time'] = pd.to_datetime(k_lines['time'], unit='ms')
    k_lines['close'] = k_lines['close'].astype(float)
    k_lines['high'] = k_lines['high'].astype(float)
    k_lines['low'] = k_lines['low'].astype(float)
    k_lines['open'] = k_lines['open'].astype(float)
    k_lines['symbol'] = symbol

    # Add TA indicators
    k_lines['ema'] = ta.trend.EMAIndicator(k_lines['close'], window=12).ema_indicator()
    k_lines['sma'] = ta.trend.SMAIndicator(k_lines['close'], window=40).sma_indicator()
    k_lines['vwma'] = ta.volume.VolumeWeightedAveragePrice(k_lines['high'], k_lines['low'], k_lines['close'], k_lines['volume'], window=12).volume_weighted_average_price()
    k_lines['rsi'] = ta.momentum.RSIIndicator(k_lines['close'], window=40).rsi()
    k_lines['adx'] = ta.trend.adx(k_lines['high'], k_lines['low'], k_lines['close'], window=40)

    results = perform_backtesting(k_lines)
    print(f"\nResults for {symbol}:")
    for result in results:
        print(f"Time: {result.time}, Asset: {result.asset}, Quantity: {result.quantity}, Side: {result.side}, Entry: {result.entry}, Take Profit: {result.take_profit}, Stop Loss: {result.stop_loss}, Result: {result.result}, Closed_by: {result.closed_by}")

    calculate_statistics(results)
