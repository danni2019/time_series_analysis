"""
author: muzexlxl
email: muzexlxl@foxmail.com
"""
import pandas as pd
from src.backtest.backtest import BackTest

if __name__ == "__main__":
    df = pd.read_parquet("../../data/data_sample/sample.parquet")

    sample = BackTest(
        id=['symbol', 'IF'],
        data_source=None,
        timeframe=None,
        direct_feed=df,
        # test_date=["2017-01-01", '2017-12-30'],
        # test_date=["2018-01-01", '2018-12-31'],
        test_date=["2019-01-01", '2019-12-31'],
        data_type='intraday_data',
        # day_close=None,
        day_close=['15:00'],
        # stop_loss=0.005,
        stop_loss=100,
        sig_0_action='close',
        # sig_0_action='hold',
        position_pctg=1,
        transaction_fee_pctg=0.000,
    )

    # set your factors here
    factors = [
        # sample.fac.factor_tmom_1
    ]
    sig_shifts = [1] * len(factors)
    params = [
        # [{'w': [15, 100]}, {'w': 15}, {'w': 100}],
        # [{'w': [15, 50]}, {'w': 15}, {'w': 100}],
        # [{'w': [27, 50]}, {'w': 27}, {'w': 100}],
        # [{'w': [27, 100]}, {'w': 27}, {'w': 100}],
        # [{'w': [15, 100]}, {'w': 15}, {'w': 15}],
        # [{'w': [27, 50]}, {'w': 27}, {'w': 27}, {'w': 27}],
    ]

    sample.multi_backtester(
        w_s=None,
        valve=1,
        func=factors,
        sig_shift=sig_shifts,
        params=params,
    )
    # sample.multi_test_signal_corr(
    #     w_s=None,
    #     valve=1,
    #     func=bias_factors,
    #     sig_shift=bias_sig_shifts,
    #     params=bias_params,
    # )
