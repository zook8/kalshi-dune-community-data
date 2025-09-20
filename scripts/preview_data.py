import pandas as pd

# Adjust the path and filename to today's date as needed
events = pd.read_csv("data/kalshi_events_20250920.csv")
#markets = pd.read_csv("data/kalshi_markets_YYYYMMDD.csv")

print("EVENTS SCHEMA AND SAMPLE:")
print(events.columns)
print(events.head(10))

#print("\nMARKETS SCHEMA AND SAMPLE:")
#print(markets.columns)
#print(markets.head(5))
