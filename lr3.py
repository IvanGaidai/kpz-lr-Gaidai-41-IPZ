import datetime
from binance.client import Client
import pandas as pd
from pandas_ta import rsi, cci, macd

def interpret_signals(data):
    rsi_signal = "Ціна впаде"
    if data["RSI"] > 70:
        rsi_signal = "Ціна буде рости"
    elif data["RSI"] > 30:
        rsi_signal = "Невідомий"

    cci_signal = "Ціна впаде"
    if data["CCI"] < -100:
        cci_signal = "Ціна буде рости"
    elif data["CCI"] < 100:
        cci_signal = "Невідомий"

    macd_signal = "Невідомий"
    if not pd.isna(data['MACD_prev']) and not pd.isna(data['MACDs_prev']):
        if data['MACD'] > data['MACDs'] and data['MACD_prev'] < data['MACDs_prev']:
            macd_signal = "Ціна буде рости"
        elif data['MACD'] < data['MACDs'] and data['MACD_prev'] > data['MACDs_prev']:
            macd_signal = "Ціна впаде"

    final_prediction = "Невідомий"
    if cci_signal != "Невідомий":
        final_prediction = cci_signal
    elif rsi_signal != "Невідомий":
        final_prediction = rsi_signal
    elif macd_signal != "Невідомий":
        final_prediction = macd_signal

    return final_prediction

def main():
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        client = Client()
        k_lines = client.get_historical_klines(
            symbol="BTCUSDT",
            interval=client.KLINE_INTERVAL_1MINUTE,
            start_str=str(yesterday),
            end_str=str(today)
        )

        col_names = ['time', 'open', 'high', 'low', 'close'] + ['extra_' + str(i) for i in range(len(k_lines[0]) - 5)]
        k_lines_df = pd.DataFrame(k_lines, columns=col_names)
        k_lines_df = k_lines_df[['time', 'open', 'high', 'low', 'close']]  # Keep only the required columns
        k_lines_df['time'] = pd.to_datetime(k_lines_df['time'], unit='ms')
        k_lines_df = k_lines_df.astype({'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float'})

        rsi_values = rsi(k_lines_df['close'])
        cci_values = cci(k_lines_df['high'], k_lines_df['low'], k_lines_df['close'])
        macd_values = macd(k_lines_df['close'])
        results = pd.concat([rsi_values, cci_values, macd_values], axis=1).dropna().reset_index(drop=True)
        results.columns = ['RSI', 'CCI', 'MACD', 'MACDh', 'MACDs']

        results['MACD_prev'] = results['MACD'].shift(1)
        results['MACDs_prev'] = results['MACDs'].shift(1)
        results["Prediction"] = results.apply(interpret_signals, axis=1)

        results.loc[:, ['RSI', 'CCI', 'MACD', 'MACDs', 'Prediction']].to_csv('prediction.csv', index=False)
        print("prediction.csv")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
