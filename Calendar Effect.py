import pandas as pd
import numpy as np
import math
import yfinance as yf
import datetime
import time
from dateutil.relativedelta import relativedelta
from dateutil import parser
from macros import ret_month_stat
from macros import ret_month_stat_date

outdir="C:/SASUniversityEdition/myfolders/ETF/"
outdir="C:/Users/zding/Dropbox/Ding0/"


# resample stock data into different frequencies 
stock_list = ['^GSPC']

df=pd.DataFrame()
for i in range(len(stock_list)):
    stock_listi = stock_list[i].replace("/","-")
    dfi = yf.download(stock_listi)
#    dfi['ticker'] = stock_listi
    df = df.append(dfi)

df_ww = df.resample('W',loffset=pd.offsets.timedelta(days=-6)).apply({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
#df_ww = df_ww[['Open', 'High', 'Low', 'Close', 'Volume']]
df_ww.to_csv(outdir+"df_ww.csv")
#df_mm = df.resample('M').apply({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
df_mm = df.resample('M').last()
df_mm['reta_mm'] = df_mm['Adj Close'].pct_change()
#df_mm['reta_mm'] = df_mm.groupby('ticker')['Adj Close'].pct_change()
df_mm['retc_mm'] = np.log(1+df_mm['reta_mm'])
df_mm['yyyy']=df_mm.index.year
df_mm['mm']=df_mm.index.month
df_yymm = df_mm.pivot(index='yyyy', columns='mm',values='retc_mm').reset_index()
df_yymm.to_csv(outdir+"sp500_yymm.csv")


# relative seasonality research of two securities
ticker1 = "^SP500TR"
ticker2 = "GLD"
ticker_list = [ticker1, ticker2]
df = yf.download(ticker_list, group_by="ticker")

# generate daily Adj Close in time series format
df_dd = df.xs('Adj Close',axis=1,level=1)
df_mm = df_dd.resample('M').last()
df_mm['ex_retc_mm'] = np.log(1+df_mm[ticker1].pct_change())-np.log(1+df_mm[ticker2].pct_change())
df_mm  = df_mm.dropna(how='any')
df_mm['yyyy']=df_mm.index.year
df_mm['mm']=df_mm.index.month
globals()[ticker1+'_'+ticker2+'_yymm'] = df_mm.pivot(index='yyyy', columns='mm',values='Adj Close').reset_index()
globals()[ticker1+'_'+ticker2+'_yymm'].to_csv(outdir+ticker1+"_"+ticker2+".csv")


# mean VIX monthly effect
ticker = "^VIX"
ticker_list = [ticker]
df = yf.download(ticker_list, group_by="ticker")
df_mm = df.resample('M').mean()
df_mm['yyyy']=df_mm.index.year
df_mm['mm']=df_mm.index.month
globals()[ticker+'_yymm'] = df_mm.pivot(index='yyyy', columns='mm',values='Adj Close').reset_index()

# mean VIX monthly effect
ticker1 = "^VIX"
ticker_list = [ticker1]
df = yf.download(ticker_list, group_by="ticker")
df_mm = df.resample('M').mean()
df_mm['yyyy']=df_mm.index.year
df_mm['mm']=df_mm.index.month
vix_yymm = df_mm.pivot(index='yyyy', columns='mm',values='Adj Close').reset_index()
vix_yymm.to_csv(outdir+"vix.csv")

# IC seasonality
IC_moma_ts = IC_moma.copy()
IC_moma_ts['yyyy'] = IC_moma_ts.index.year
IC_moma_ts['mm'] = IC_moma_ts.index.month
IC_moma_ts = IC_moma_ts.pivot(index='yyyy', columns='mm', values='moma_'+str(momMonths)+'_std_252').reset_index()

with pd.ExcelWriter(outdir+"Calendar Effect".xlsx") as writer:    
    vix_yymm.to_excel(writer, sheet_name='vix')
    globals()[ticker1+'_'+ticker2+'_yymm'].to_csv(outdir+ticker1+"_"+ticker2+".csv")
    IC_moma_ts.to_excel(writer, sheet_name='IC_moma')
    


ETF_list = ["FDN", "GDX", "IBB", "IGE", "IGV", "IHF", "IHI", "ITA", "VDC", "IYT"
           ,"KBE", "KIE", "KRE", "OIH", "PJP", "QQQ", "VNQ", "VOX", "SPY", "SOXX"
           ,"XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY", "XES"
           ,"XHB", "XOP", "XRT", "IWV", "IWB", "IWF", "IWD", "IWN", "FXD", "VCR"
           ,"GLD", "COPX", "PICK", "SLV", "TLT", "AGG", "HYG", "MTUM", "VLUE","QUAL","USMV"
#           ,"ITEQ", "KURE", "SKYY","CLOU"
            ]

# read in ETF Adj Close Price data from Yahoo Finance
df = yf.download(ETF_list, group_by="ticker")

ETF_dd = df.xs('Adj Close',axis=1,level=1).stack().reset_index()
ETF_dd.columns = ['date','ticker','price']

ETF_mm = ETF_dd.groupby(['ticker', ETF_dd['date'].dt.year, ETF_dd['date'].dt.month]).tail(1)
ETF_mm.sort_values(by=['ticker','date'],inplace=True)
ETF_mm['reta_mm'] = ETF_mm.groupby('ticker')['price'].pct_change()
ETF_mm['retc_mm'] = np.log(1+ETF_mm['reta_mm'])

ETF_mm['yyyy'] = ETF_mm['date'].dt.year
ETF_mm['mm'] = ETF_mm['date'].dt.month
ETF_month = ETF_mm[['ticker','yyyy','mm','retc_mm']]

ETF_month_summary = ret_month_stat(ETF_month, 'ticker', 'yyyy', 'mm', 'retc_mm', 12)
ETF_month_summary.to_csv(outdir+"ETF_summary.csv")

ETF_month.set_index(['ticker','yyyy','mm'], inplace=True)
ETF_month = ETF_month['retc_mm'].unstack('mm').reset_index()
stock_list = list(set(ETF_month_summary['ticker'].tolist()))

with pd.ExcelWriter(outdir+"ETF_month.xlsx") as writer:
    
    for i in range(len(stock_list)):
    #    stock_listi = stock_list[i]    
        df = ETF_month[ETF_month['ticker']==stock_list[i]]
    #    df.to_csv(outdir+'test_'+stock_list[i]+'.csv')
        df.to_excel(writer,sheet_name=stock_list[i])
   
 
SPY = ETF_mm[ETF_mm['ticker']=='SPY'][['date','retc_mm']]
SPY.columns = ['date','SPY']
ETF_month_ExSP500 = ETF_mm.merge(SPY, how='inner', on='date')
ETF_month_ExSP500['exretc_mm'] = ETF_month_ExSP500['retc_mm']-ETF_month_ExSP500['SPY']

#ETF_month_ExSP500 = ETF_mm.merge(bmk_mm[['^SP500TR']], how='inner', left_on='date', right_index=True)
#ETF_month_ExSP500['exretc_mm'] = ETF_month_ExSP500['retc_mm']-ETF_month_ExSP500['^SP500TR']

ETF_month_ExSP500 = ETF_month_ExSP500[['ticker', 'yyyy', 'mm',  'exretc_mm']]

ETF_month_ExSP500_summary = ret_month_stat(ETF_month_ExSP500, 'ticker', 'yyyy', 'mm',  'exretc_mm', 12)

ETF_month_ExSP500_summary.to_csv(outdir+"ETF_month_ExSP500_summary.csv")

ETF_month_ExSP500.set_index(['ticker','yyyy','mm'], inplace=True)
ETF_month_ExSP500 = ETF_month_ExSP500['exretc_mm'].unstack('mm').reset_index()
stock_list = list(set(ETF_month_ExSP500['ticker'].tolist()))

with pd.ExcelWriter(outdir+"ETF_month_ExSP500.xlsx") as writer:
    
    for i in range(len(stock_list)):
    #    stock_listi = stock_list[i]    
        df = ETF_month_ExSP500[ETF_month_ExSP500['ticker']==stock_list[i]]
    #    df.to_csv(outdir+'test_'+stock_list[i]+'.csv')
        df.to_excel(writer,sheet_name=stock_list[i])




# China asset allocation calendar effect
df_mm=pd.read_excel('C:/Users/zding/Dropbox/Ding7Shanghai/Research/Data/AAdata_reta.xlsx', sheet_name='AAdata_retc')
df_mm['stock_bond'] = df_mm['stock']-df_mm['bond']
df_mm['stock_gold'] = df_mm['stock']-df_mm['gold']
df_mm['yyyy']=df_mm['date'].dt.year
df_mm['mm']=df_mm['date'].dt.month
stock_bond = df_mm.pivot(index='yyyy', columns='mm',values='stock_bond').reset_index()
stock_bond.to_csv(outdir+"stock_bond.csv")
stock_gold = df_mm.pivot(index='yyyy', columns='mm',values='stock_gold').reset_index()
stock_gold.to_csv(outdir+"stock_gold.csv")


df_mm=pd.read_excel('C:/Users/zding/Dropbox/Ding7Shanghai/Research/Data/模型数据.xlsx', sheet_name='大类资产月度对数收益')

