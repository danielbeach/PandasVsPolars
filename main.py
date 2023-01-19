import pandas as pd
import polars as pl


def main(local_data: str = 'data/2022-07-01.csv'):
    # reading and displaying CSV files
    # Pandas
    """
    pd_df = pd.read_csv(local_data, header=0)
    print(pd_df)
    # Polars
    pl_df = pl.read_csv(local_data)  # polars can scan and not read.
    print(pl_df)


    # writing CSV to Parquet, then reading Parquet (with Partitions)
    # Pandas
    pd_df = pd.read_csv(local_data, header=0)
    pd_df.to_parquet('data/pandas_parquet/', partition_cols=['date'])
    df_pd = pd.read_parquet('data/pandas_parquet')
    print(df_pd)
    # Polars
    pl_df = pl.read_csv(local_data)  # polars can scan and not read.
    pl_df.write_parquet('data/polars_parquet') # <- unable to write partitions
    df_pl = pl.read_parquet('data/polars_parquet', use_pyarrow=True)
    print(df_pl)

    # Messing with Columns
    # Pandas
    pd_df = pd.read_csv(local_data, header=0)
    # rename
    pd_df.rename({'date': 'date_of_capture', 'serial_number': 'sn'}, axis=1, inplace=True)
    print(pd_df.columns)

    # Polars
    pl_df = pl.read_csv(local_data, parse_dates=True)
    # rename
    pl_df = pl_df.rename({"date": "date_of_capture", "serial_number": "sn"})
    print(pl_df.columns)


    # Assigning value to new column
    # Pandas
    pd_df = pd.read_csv(local_data, header=0)
    pd_df['year'] = pd.DatetimeIndex(pd_df['date']).year
    pd_df = pd_df.drop(['smart_1_normalized', 'smart_1_raw'], axis=1)
    print(pd_df.columns)


    # Polars
    pl_df = pl.read_csv(local_data, parse_dates=True)
    pl_df = pl_df.with_column(pl.col("date").dt.year().alias('year'))
    pl_df = pl_df.drop(['smart_1_normalized', 'smart_1_raw'])
    print(pl_df.columns)
    """

    # Aggregation and Grouping
    # Pandas
    pd_df = pd.read_csv(local_data, header=0)
    pd_df = pd_df.groupby(['date'])['failure'].count()
    print(pd_df.head(1))

    # Polars
    pl_df = pl.read_csv(local_data, parse_dates=True)
    pl_df = pl_df.groupby(pl.col('date')).agg(pl.count('failure'))
    print(pl_df)


if __name__ == '__main__':
    main()

