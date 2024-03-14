import pandas as pd
import datetime as dat
data_frame = pd.DataFrame(columns=["year", "month", "day", "hour", "minute", "second"]) 
now = dat.datetime.now()
data_frame.loc[len(data_frame)] = [now.year, now.month, now.day, now.hour, now.minute, now.second]
print(data_frame)
data_frame.to_csv("filename.csv", index=False)