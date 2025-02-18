from TradingviewData import TradingViewData,Interval

request = TradingViewData()


relience_data = request.get_hist(
    symbol='XLMUSD',
    exchange='BINANCE',
    interval=Interval.daily, 
    n_bars=1461  # Número aproximado de días en 4 años
)

print(relience_data)
relience_data.to_csv('Stellar_data.csv')