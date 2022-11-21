import multiprocessing as mp
import pandas as pd
import datetime as dt
from pandas.api.types import is_numeric_dtype
import sys
    
def condense_month(df, ind):
    ind_limit = len(df.index)
    start_date = df.loc[ind, 'date']
    numCols = len(df.columns)
    
    accums = [None for _ in range(len(df.columns))]
    count = 0
    
    for i in range(ind, ind_limit):
        cur_date = df.loc[i, 'date']
        if cur_date.month != start_date.month or cur_date.year != start_date.year:
            break
        for col_ind in range(numCols):
            val = df.iloc[i, col_ind]
            if (not pd.isnull(val)) and is_numeric_dtype(val):
                if accums[col_ind] is None:
                    accums[col_ind] = val
                else:
                    accums[col_ind] += val
        count += 1
    
    av_args = [(df, i, accums[i], ind, count) for i in range(len(accums))]
    for i, v in enumerate(accums):
        accums[i] = float(v) / float(count) if v is not None else df.iloc[ind, i]
    return accums
        
        
    
def get_month_inds(df):
    numrows = len(df.index)
    inds = []
    # cur_month = df.loc[0, 'date'].month
    # cur_start = 0
    cur_month = None
    cur_year = None
    cur_start = None
    for i in range(numrows):
        cur_date = df.loc[i, 'date']
        if cur_date.month != cur_month or cur_date.year != cur_year:
            inds.append(i)
            cur_month = cur_date.month
            cur_year = cur_date.year
            cur_start = i
    return inds

def format_condense(t):
    return condense_month(t[0], t[1])

def condense_months(df, inds):
    numMonths = len(inds)
    args_arr = [(df, inds[i]) for i in range(numMonths)]
    
    with mp.Pool(processes=16) as pool:
        result = pool.map(format_condense, args_arr)
    result = pd.DataFrame(result)
    result.columns = list(df.columns)
    return result

def build_initial_table(filename):
    covid_data = pd.read_csv(filename, parse_dates=['date'], infer_datetime_format=True)
    return covid_data

def main():
    if len(sys.argv > 1):
        fname = sys.argv[1]
    else:
        fname = "Tennessee_Covid_Data.csv"
    if len(sys.argv > 2):
        out_fname = sys.argv[2]
    else:
        out_fname = "Condensed_Tennessee_Covid_Data.csv"
    covid_data = build_initial_table(fname)
    month_inds = get_month_inds(covid_data)
    condensed = condense_months(covid_data, month_inds)
    condensed.to_csv(out_fname)

if __name__ == "__main__":
    main()