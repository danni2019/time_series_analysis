"""
author: muzexlxl
email: muzexlxl@foxmail.com
"""

from src.backtest.jab import BackTester

if __name__ == "__main__":
    sample = BackTester(
        id=['symbol', 'IF'],
        # id=['code', 'RB2010'],
        data_source='xx',
        timeframe='T1',
        test_date=["2021-04-30", '2021-05-10'],
        data_type='intraday_data',
        # day_close=None,
        day_close=['15:00'],
        # stop_loss=0.005,
        stop_loss=100,
        sig_0_action='close',
        # sig_0_action='hold',
        position_pctg=1,
        transaction_fee_pctg=0.0000,
    )

    # # set neutral factors as signal filter
    # neut_factors = [
    #     sample.fac.factor_tmom_neut_01
    #
    # ]
    # neut_sig_shifts = [1] * len(neut_factors)
    # neut_params = [
    #     {'w': 27},
    # ]
    # sample.make_neut_signal(
    #     valve=1,
    #     w_s=None,
    #     func=neut_factors,
    #     sig_shift=neut_sig_shifts,
    #     kwargs=neut_params
    # )

    # set your biased factors here
    bias_factors = [
        sample.fac.factor_tmom_bias_01,
    ]
    bias_sig_shifts = [1] * len(bias_factors)
    bias_params = [
        # pass in factor parameters like this:
        # [{'w': [15, 50]}, {'w': 15}, {'w': 100}],
        # [{'w': [27, 50]}, {'w': 27}, {'w': 100}],
        # [{'w': 3}, {'w': 15}, {'w': 45}],
        # [{'w': 7}, {'w': 27}, {'w': 90}],
        # [{'w': 15}, {'w': 45}, {'w': 120}],
        # [{'w': 27}, {'w': 45}, {'w': 90}],
        # [{'w': 45}, {'w': 90}, {'w': 120}],
        [{'w': 7}, {'w': 15}],
    ]

    sample.multi_backtester(
        w_s=None,
        valve=1,
        func=bias_factors,
        sig_shift=bias_sig_shifts,
        params=bias_params,
    )
    # sample.multi_test_signal_corr(
    #     w_s=None,
    #     valve=1,
    #     func=bias_factors,
    #     sig_shift=bias_sig_shifts,
    #     params=bias_params,
    # )
