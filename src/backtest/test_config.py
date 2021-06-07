test_settings = {
    'test_name': 'default',
    'tests': [
        {
            'instrument': 'IF',  # 你需要交易的标的代码，例如 300股指 -> IF
            'md': 'index300_main_430-510',  # 你的行情文件名
            'factors': 'factor_tmom_T1_RTN_60',  # 你的因子文件名 （默认情况下，因子文件与信号文件是同名的）
            'sig_shift': 1,  # 信号推移量 不小于1的整数
            'timeframe': 'T1',  # 回测时间周期： T1 即 1分钟，D1 即 1天
            'datasource': 'XX',  # 数据来源 可省略
            'friction_loss': 0.4,  # 交易摩擦损失（滑点、手续费等等）
            'return_base': 'pre_close',  # 计算收益的参照：可选：open， pre_close
            'position_pctg': 1,  # 仓位，(0, 1], 1即为满仓
            'stop_loss': 999999,  # 按照点差计算的止损，如果不需要止损，就将这个数值调到尽可能大。
            'sig_0_action': 'close',  # 0信号的处理方式，可选：close, hold
            'day_close': "15:00",  # 每个交易日的清仓时间，如果没有则传入""，如果有多个时间则用空格分隔。
            "period": "2021-04-30 2021-05-10"  # 交易起止日期 用空格分隔。
        },{
            'instrument': 'IF',
            'md': 'index300_main_430-510',
            'factors': 'factor_tmom_T1_RTN_60',
            'sig_shift': 1,
            'timeframe': 'T1',
            'datasource': 'XX',
            'friction_loss': 0.4,
            'return_base': 'pre_close',
            'position_pctg': 1,
            'stop_loss': 999999,
            'sig_0_action': 'close',
            'day_close': "15:00",
            "period": "2021-04-30 2021-05-10"
        },{
            'instrument': 'IF',
            'md': 'index300_main_430-510',
            'factors': 'factor_tmom_T1_RTN_60',
            'sig_shift': 1,
            'timeframe': 'T1',
            'datasource': 'XX',
            'friction_loss': 0.4,
            'return_base': 'pre_close',
            'position_pctg': 1,
            'stop_loss': 999999,
            'sig_0_action': 'close',
            'day_close': "15:00",
            "period": "2021-04-30 2021-05-10"
        },
    ]
}