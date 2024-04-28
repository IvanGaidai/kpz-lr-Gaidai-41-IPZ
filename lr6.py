import pandas as pd
import ta
from binance import Client
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

            if (signal.side == "sell" and data_slice.iloc[candle_id]["low"] <= signal.take_profit) or (
                    signal.side == "buy" and data_slice.iloc[candle_id]["high"] >= signal.take_profit):
                signal.result = signal.take_profit - signal.entry if signal.side == 'buy' else (
                        signal.entry - signal.take_profit)
            elif (signal.side == "sell" and data_slice.iloc[candle_id]["high"] >= signal.stop_loss) or (
                    signal.side == "buy" and data_slice.iloc[candle_id]["low"] <= signal.stop_loss):
                signal.result = signal.stop_loss - signal.entry if signal.side == 'buy' else (
                        signal.entry - signal.stop_loss)

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
    print(f"{calculate_pnl(trade_list)=}")
    print(f"{profit_factor(trade_list)=}")


def profit_factor(trade_list: List[Signal]):
    total_loss = 0
    total_profit = 0
    for trade in trade_list:
        if trade.result > 0:
            total_profit += trade.result
        else:
            total_loss += trade.result
    return total_profit / abs(total_loss)


# Здесь вы должны вызвать функцию perform_backtesting и напечатать результаты


def create_signals(k_lines):
    signals = []
    for i in range(len(k_lines)):
        current_price = k_lines.iloc[i]['close']
        if k_lines.iloc[i]['cci'] < -100 and k_lines.iloc[i]['adx'] > 25:
            signal = 'sell'
        elif k_lines.iloc[i]['cci'] > 100 and k_lines.iloc[i]['adx'] > 25:
            signal = 'buy'
        else:
            continue  # Пропускаем создание сигналов без активных условий

        if signal == "buy":
            stop_loss_price = round((1 - 0.01) * current_price, 1)
            take_profit_price = round((1 + 0.015) * current_price, 1)
        elif signal == "sell":
            stop_loss_price = round((1 + 0.01) * current_price, 1)
            take_profit_price = round((1 - 0.015) * current_price, 1)

        signals.append(Signal(
            k_lines.iloc[i]['time'],
            'BTCUSDT',
            100,
            signal,
            current_price,
            take_profit_price,
            stop_loss_price,
            None, None
        ))

    return signals


client = Client()
k_lines = client.get_historical_klines(
    symbol="BTCUSDT",
    interval=Client.KLINE_INTERVAL_1MINUTE,
    start_str="1 week ago UTC",
    end_str="now UTC"
)

# Создание DataFrame
k_lines = pd.DataFrame(k_lines,
                       columns=['time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume',
                                'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                                'ignore'])
k_lines['time'] = pd.to_datetime(k_lines['time'], unit='ms')
k_lines['close'] = k_lines['close'].astype(float)
k_lines['high'] = k_lines['high'].astype(float)
k_lines['low'] = k_lines['low'].astype(float)
k_lines['open'] = k_lines['open'].astype(float)

k_lines['adx'] = ta.trend.ADXIndicator(k_lines['high'], k_lines['low'], k_lines['close']).adx()
k_lines['cci'] = ta.trend.CCIIndicator(k_lines['high'], k_lines['low'], k_lines['close']).cci()

results = perform_backtesting(k_lines)
for result in results:
    print(f"Time: {result.time}, Asset: {result.asset}, Quantity: {result.quantity}, Side: {result.side}, "
          f"Entry: {result.entry}, Take Profit: {result.take_profit}, Stop Loss: {result.stop_loss}, Result: {result.result}, Closed_by: {result.closed_by}")
calculate_statistics(results)