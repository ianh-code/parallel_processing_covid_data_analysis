import pandas as pd
import multiprocessing as mp
import csv, sys, time
​
def findProbs(dataFile, yes, no, notReported):
    data = list(csv.reader(open(dataFile)))
​
    yesCol = yes
    noCol = no
    notReportedCol = notReported
​
    total = 0
    numYes = 0
    numNo = 0
    numNotReported = 0
    probYes = 0
    probNotReported = 0
    p_Yes = []
    p_NotReported = []
​
    for i in range(1, 924):
        numYes = int(data[i][yesCol])
        numNo = int(data[i][noCol])
        numNotReported = int(data[i][notReportedCol])
        total = numYes + numNo + numNotReported
        probYes = numYes / total
        probNotReported = numNotReported / total
        p_Yes.append(probYes)
        p_NotReported.append(probNotReported)
        
    condensedCol=(p_Yes, p_NotReported)
​
    return condensedCol
​
def format_findProbs(args):
    return findProbs(args[0], args[1], args[2], args[3])
​
def main():
    dataFile = sys.argv[1]
​
    args = [(dataFile, 1, 2, 3), (dataFile, 4, 5, 6)]
​
    condensedCols = [0, 0, 0, 0]
​
    with mp.Pool(processes=16) as pool:
        result = pool.map(format_findProbs, args)
​
    condensedCols[0] = result[0][0]
    condensedCols[1] = result[0][1]
    condensedCols[2] = result[1][0]
    condensedCols[3] = result[1][1]
    
    # Pandas data frame
    df = pd.read_csv(dataFile, parse_dates=["date"], infer_datetime_format = True)
    
    # Add new condensed columns to dataframe
    df.insert(loc=1,column="p(critical_staffing_shortage)", value=condensedCols[0])
    df.insert(loc=2,column="p(critical_staffing_shortage_not_reported)",value=condensedCols[1])
    df.insert(loc=3,column="p(critical_staffing_shortage_anticipated)",value=condensedCols[2])
    df.insert(loc=4,column="p(critical_staffing_shortage_anticipated_not_reported)",value=condensedCols[3])
    
    # Delete old columns from dataframe
    df.drop('critical_staffing_shortage_today_yes', inplace=True, axis=1)
    df.drop('critical_staffing_shortage_today_no', inplace=True, axis=1)
    df.drop('critical_staffing_shortage_today_not_reported', inplace=True, axis=1)
    df.drop('critical_staffing_shortage_anticipated_within_week_yes', inplace=True, axis=1)
    df.drop('critical_staffing_shortage_anticipated_within_week_no', inplace=True, axis=1)
    df.drop('critical_staffing_shortage_anticipated_within_week_not_reported',inplace=True,axis=1)
    
    print(df)
​
    return 0
​
if __name__ == "__main__":
    startTime = time.time()
    main()
    endTime = time.time()
    print("%f seconds" %(endTime-startTime))
