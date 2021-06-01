"""
author: muzexlxl
email: muzexlxl@foxmail.com

time series factors
bias: -1 0 1
neut: 1, 0

"""

import pandas as pd
import numpy as np


def factor_tmom_T1_RTN_60(df: pd.DataFrame):
    """
    factor example
    """
    factor = df['return'].rolling(60).sum()
    return factor



if __name__ == "__main__":
    df = pd.read_parquet("../../data/futures/raw/intraday_data/index300_main_430-510.parquet")
    df['return'] = df['close'].diff()
    df['factor'] = factor_tmom_T1_RTN_60(df)
    df['signal'] = np.sign(df['factor'])

    df[['factor']].to_parquet('../../factor/IF/factor_tmom_T1_RTN_60.parquet')
    df[['signal']].to_parquet('../../signal/IF/factor_tmom_T1_RTN_60.parquet')
