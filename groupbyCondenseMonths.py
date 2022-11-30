import pandas as pd
import datetime as dt
import sys
import time

def my_month_group(df, idx):
    this_date = df.loc[idx, 'date']
    return (this_date.year, this_date.month)

def condense_months(df):
    return df.groupby(lambda x: my_month_group(df, x)).mean()

def build_initial_table(filename):
    covid_data = pd.read_csv(filename, parse_dates=['date'], infer_datetime_format=True)
    return covid_data

def main():
    if len(sys.argv) > 1:
        fname = sys.argv[1]
    else:
        fname = "Tennessee_Covid_Data.csv"
    if len(sys.argv) > 2:
        out_fname = sys.argv[2]
    else:
        out_fname = "Condensed_Tennessee_Covid_Data.csv"
    covid_data = build_initial_table(fname)

    condensed = condense_months(covid_data)
    condensed.to_csv(out_fname)

if __name__ == "__main__":
    start_t = time.time()
    main()
    end_t = time.time()
    print(f"{end_t - start_t} seconds")