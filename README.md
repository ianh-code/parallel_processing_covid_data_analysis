# Parallel Processing Covid Data Analysis

Project contributors: Patrick Burnett, Warren Russel, and Ian Hurd

## Horizontal Condense

## Vertical Condense

The programs **parallelCondenseMonths.py** and **groupbyCondenseMonths.py** will take a csv file of data sorted by date, and with no repeat days. It will take the mean of each column's data for each month, and output a new CSV file with one row for each month, with each numerical data field containing the mean of that field over the course of the given month.

Note that for the initial covid data set, there were multiple rows for the same day but from different states. This would have resulted in meaningless data. We have filtered out data from other states, so there is only Tennessee data and no repeat dates.

To use the program, run it with python 3. The program takes 0, 1, or 2 command line arguments. With 2 command line arguments, the first argument will be the input file name, and the second argument will be the output file name. With one argument, the argument will be the input file name, and a default name will be used as the output file name. With no arguments, default filenames will be used for both the input and the output.

The default input filename is **Tennessee_Covid_Data.csv** and the default output filename is **Condensed_Tennessee_Covid_Data.csv**.

Both programs do the same thing, but **parallelCondenseMonths.py** runs with a slower algorithm but in parallel, and **groupbyCondenseMonths.py** runs linearly, but with Pandas' built-in highly-optimized algorithms.

In **parallelCondenseMonths.py**, you can change the number of processors by going into the source code and changing the value of the environment variable **num_procs**.

Each program outputs time taken.
