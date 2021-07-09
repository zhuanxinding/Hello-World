# to add this macro to the automatic run folder, do the following
# import sys
# sys.path
# sys.path.append(r'C:\Users\zding\Dropbox\Code\Python\macros')
# the last line add the folder the code is located as a path
# to remove the path, do the following
# sys.path.remove(r'C:\Users\zding\Dropbox\Code\Python\macros')

# multi-level index reference
# df.xs('one',level='second')  pick the rows with index 'one'
# df.loc(axis=0)[:,['one']]
# df.loc[pd.IndexSlice[:,['one']],]
# df.xs('one',axis=1,level='second')  pick the columns with index 'one'
# df.swaplevel(0,1,axis=0)
# df.reorder_levels([1, 0], axis=0)
# refer to the column:  df['bar'] where bar is the column name
# refer to the row:  df.loc['bar'] where bar is the row name in the level 0 index
# df.swaplevel(0,1,axis=0).loc['one'] to get the level 1 rows with name 'one'

import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.tsa.filters.hp_filter import hpfilter
    
# given a dataframe with identifier, date_var, ret_var as columns, calculate each security return's summary stats
def ret_summary_stat(df, identifier, date_var, ret_var, annual_scale):
    
    df_ret = df[[identifier, date_var, ret_var]]
    df_ret.sort_values(by=[identifier, date_var],inplace=True)    
    df_ret.set_index([identifier, date_var], inplace=True)
    
    # return statistics
    ret_summary = df_ret.groupby(identifier)[ret_var].describe()
    ret_summary['mean'] = ret_summary['mean']*annual_scale
    ret_summary['std'] = ret_summary['std']*np.sqrt(annual_scale)    
    ret_summary['down_std'] = df_ret[df_ret<0].groupby(identifier).std()*np.sqrt(annual_scale)
    ret_summary['Sharpe Ratio'] = ret_summary['mean']/ret_summary['std']
    ret_summary['Sortino Ratio'] = ret_summary['mean']/ret_summary['down_std']
    ret_summary['HitRatio'] = df_ret.gt(0).groupby(identifier).mean()
 
    # return drawdowns
    df_ret[ret_var+'_cumu'] = df_ret.groupby(identifier)[ret_var].cumsum()
    df_ret[ret_var+'_cumu_max'] = df_ret.groupby(identifier)[ret_var+'_cumu'].cummax()
    df_ret[ret_var+'_cumu_max'] = np.where(df_ret[ret_var+'_cumu_max']<0, 0, df_ret[ret_var+'_cumu_max'])
    df_ret['drawdown'] = np.exp(df_ret[ret_var+'_cumu']-df_ret[ret_var+'_cumu_max'])-1
    
    # calculate maximum drawdown and Calmar Ratio
    ret_summary['maxdrawdown'] = df_ret.groupby(identifier)['drawdown'].min()
    ret_summary['Calmar Ratio'] = ret_summary['mean']/abs(ret_summary['maxdrawdown'])
    
    return ret_summary, df_ret


#ETF_dd_summary, ETF_dd_drawdown = ret_summary_stat(ETF_dd_panel, 'ticker', 'date', 'retc_dd', 252)   

# maximum drawdown function from https://www.codearmo.com/blog/sharpe-sortino-and-calmar-ratios-python
#def max_drawdown(return_series):
#    comp_ret = (return_series+1).cumprod()
#    peak = comp_ret.expanding(min_periods=1).max()
#    dd = (comp_ret/peak)-1
#    return dd.min()
#max_drawdowns = df.apply(max_drawdown,axis=0)


# given a dataframe with identifier, year_var, month_var, ret_var as columns, calculate return's calendar month stats
def ret_month_stat(df, identifier, year_var, month_var, ret_var, annual_scale):
    
    df_temp = df[[identifier, year_var, month_var, ret_var]]
    df_temp.set_index([identifier, year_var, month_var], inplace=True)
    df_temp = df_temp[ret_var].unstack(month_var).reset_index()
    
    ret_month_summary=df_temp.drop(year_var,axis=1).groupby(identifier).describe().unstack().unstack(level=1)
    ret_month_summary['mean'] = ret_month_summary['mean']*annual_scale
    ret_month_summary['std'] = ret_month_summary['std']*np.sqrt(annual_scale)
    ret_month_summary['Sharpe Ratio'] = ret_month_summary['mean']/ret_month_summary['std']
    ret_month_summary['HitRatio'] = df_temp.set_index([identifier, year_var]).gt(0).groupby(identifier).mean().stack().swaplevel(0,1)
    ret_month_summary = ret_month_summary.stack().unstack(level=0).reset_index()
    #ret_month_summary=ret_month_summary.sort_values(by=[identifier]).sort_index()

    return ret_month_summary

#ETF_month_summary = ret_month_stat(ETF_month, 'ticker', 'yyyy', 'mm', 'retc_mm', 12)


# same as ret_month_stat but the input dataframe only has identifier, date_var, ret_var as columns, calculate return's calendar month stats
def ret_month_stat_date(df, identifier, date_var, ret_var, annual_scale):
    
    year_var = 'yyyy'
    month_var = 'mm'
    df_temp = df[[identifier, date_var, ret_var]]
    df_temp[year_var] = df_temp[date_var].dt.year
    df_temp[month_var] = df_temp[date_var].dt.month

    df_temp.set_index([identifier, year_var, month_var], inplace=True)
    df_temp = df_temp[ret_var].unstack(month_var).reset_index()
    
    ret_month_summary=df_temp.drop(year_var,axis=1).groupby(identifier).describe().unstack().unstack(level=1)
    ret_month_summary['mean'] = ret_month_summary['mean']*annual_scale
    ret_month_summary['std'] = ret_month_summary['std']*np.sqrt(annual_scale)
    ret_month_summary['Sharpe Ratio'] = ret_month_summary['mean']/ret_month_summary['std']
    ret_month_summary['HitRatio'] = df_temp.set_index([identifier, year_var]).gt(0).groupby(identifier).mean().stack().swaplevel(0,1)
    ret_month_summary = ret_month_summary.stack().unstack(level=0).reset_index()
    #ret_month_summary=ret_month_summary.sort_values(by=[identifier]).sort_index()

    return ret_month_summary

#ETF_month_summary = ret_month_stat_date(ETF_month, 'ticker', 'date', 'retc_mm', 12)


# given a dataframe with group df_hp and a col_name to do HP-filtering, this function does a expanding window HP filter for each group
# HP filter (Compared with EViews and the result is correct)
def hpfilter_expanding(df_hp, col_name, min_obs=20, lamb=7000):
    
    df_hp = df_hp.assign(hp_cycle=np.nan,hp_trend=np.nan)
    for i in range(min_obs-1,len(df_hp)):
        cycle, trend = hpfilter(df_hp.loc[df_hp.index[0]:df_hp.index[0]+i][col_name], lamb=lamb)
        df_hp.loc[df_hp.index[0]+i, 'hp_cycle'] = cycle.iloc[-1]
        df_hp.loc[df_hp.index[0]+i, 'hp_trend'] = trend.iloc[-1]
    return df_hp

#hp_min_obs=12
#hp_lambda=7000
#ETF_mm = ETF_mm.groupby('ticker').apply(lambda x: hpfilter_expanding(x,'logprice_lag1day',hp_min_obs,hp_lambda))
#ETF_mm = ETF_mm.reset_index(drop=True)


# do a rolling regression and prediction
def rolling_fit_predict(df,Y,Xs,n_rolling,n_min):
    
    model = RollingOLS(df[Y], sm.add_constant(df[Xs]), window=n_rolling, min_nobs=n_min)
    params = model.fit(params_only=True).params
    params_shift = params.shift(1)
    df_predicator = sm.add_constant(df[Xs])
    df_pred = df_predicator*params_shift
    df_pred[Y+'_pred'] = df_pred.sum(axis=1,min_count=1)
    
    return params,params_shift,df_predicator,df_pred


def expanding_fit_predict_0(df,Y,Xs,n_min=None):
    
    if n_min is None:
        n_min = len(Xs)+1
    
    df_predicator = sm.add_constant(df[Xs])
    params = pd.DataFrame(index=df.index,columns=df_predicator.columns)
    for i in range(n_min,len(df)):
        df1 = df.iloc[:i+1].dropna()
        model = sm.OLS(df1[Y], sm.add_constant(df1[Xs]))
        params.iloc[i] = model.fit(params_only=True).params
    params = params.astype('float')
    params_shift = params.shift(1)   
    df_pred = df_predicator*params_shift
    df_pred[Y+'_pred'] = df_pred.sum(axis=1,min_count=1)
   
    return params,params_shift,df_predicator,df_pred


def expanding_fit_predict(df,Y,Xs,n_min=None):
    
    if n_min is None:
        n_min = len(Xs)+1
 
    df_tmp = df[[Y]+Xs]
    df_tmp.dropna(how='any', inplace=True)
    
    model = sm.RecursiveLS(df_tmp[Y], sm.add_constant(df_tmp[Xs]))
    arr_params = model.fit().recursive_coefficients.filtered.T
    params = pd.DataFrame(arr_params,columns=['const']+Xs,index=df_tmp.index)
    params.iloc[:n_min,] = np.NaN
    params = params.merge(df[[]].copy(),how='outer',left_index=True,right_index=True)
    params_shift = params.shift(1)
    
    df_predicator = sm.add_constant(df[Xs])
    df_pred = df_predicator*params_shift
    df_pred[Y+'_pred'] = df_pred.sum(axis=1,min_count=n_min)    # min_count=1 so that it will not sum all NaNs to 0
    
    return params,params_shift,df_predicator,df_pred



# the following business day logic is from here
# https://stackoverflow.com/questions/9187215/datetime-python-next-business-day/42824111
import datetime
import holidays

def next_business_day():
    next_day = datetime.date.today() + datetime.timedelta(days=1)
    while next_day.weekday() in holidays.WEEKEND or next_day in holidays.US():
        next_day += datetime.timedelta(days=1)
    return next_day

def next_weekday():
    next_day = datetime.date.today() + datetime.timedelta(days=1)
    while next_day.weekday() in holidays.WEEKEND:
        next_day += datetime.timedelta(days=1)
    return next_day

# the following code generates dummies according to Chinese lunar year calendar
import  sxtwl
def lunar_dummy():
    lunar = sxtwl.Lunar()  #实例化日历库
    
    df = pd.DataFrame(index=pd.date_range('2007/1','2023/1',freq='M'))
    df['dummy']=np.where(df.index.month==2,1,0)
    
    for i in range(len(df)):
        try:
            LunarDay = lunar.getDayByLunar(df.index[i].year, 1, 1 , False) 
        except:
            LunarDay = lunar.getDayByLunar(df.index[i].year, 1, 1 , True) 
        if datetime(LunarDay.y,LunarDay.m,LunarDay.d) >=  datetime(LunarDay.y,2,10) and df.index[i].month==3:
            df['dummy'].iloc[i] = 1
    return df
