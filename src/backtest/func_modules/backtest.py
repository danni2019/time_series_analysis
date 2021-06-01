"""
backtest logic
another revision.
"""

import pandas as pd
import numpy as np
import os
import logging
import pickle
import hashlib
from datetime import datetime, time
import src.data.redis_handle as rh

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

fp = os.path.dirname(__file__)

log_path = os.path.join(fp, "../../../docs/backtest/backtest_log.txt")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
filing = logging.FileHandler(filename=log_path)
streaming = logging.StreamHandler()
formatter = logging.Formatter(fmt="%(asctime)s - %(filename)s - %(levelname)s - %(message)s",
                              datefmt="%Y-%m-%d %H:%M:%S")
filing.setFormatter(formatter)
streaming.setFormatter(formatter)
logger.addHandler(filing)
logger.addHandler(streaming)


class BackTest:

    def __init__(
            self,
            meta_data: pd.DataFrame,
            factor_data: pd.DataFrame,
            signal_data: pd.DataFrame,
            settings: dict,
    ):
        """
        Test it before use it, bro!
        Args:
            settings:
                day_close: list of daily close time
                stop_loss: stop loss based on the minimum price fluctuation
                sig_0_action: action to take on signal 0, hold or close
                position_pctg: position percentage
                friction_loss: trade friction loss including transaction fees and slipperage, etc..
                return_base: the base price on which to calc return, pre_close or open
        """
        logger.info("Initiating Backtest.")
        df = pd.concat([meta_data, factor_data, signal_data], axis=1)
        self.settings = settings

        self.timeframe = settings.get('timeframe')
        self.mq_name = settings.get('mq', 'default')
        self.friction_loss = settings.get('friction_loss', 0)
        self.return_base = settings.get('return_base', 'pre_close')
        self.pos_ = settings.get('position_pctg', 1)
        self.stop_loss = settings.get('stop_loss', 100)
        self.action_on_0 = settings.get('sig_0_action', 'close')
        day_close = settings.get('day_close', None)
        if day_close is not None:
            self.trade_flag = [time.fromisoformat(t) for t in day_close.split(' ')]
        else:
            self.trade_flag = 0

        self.period = settings.get("period", None)

        df.sort_index(ascending=True, inplace=True)
        df['date'] = df.index.date if self.timeframe[0] == 'T' else df.index

        if self.period is not None:
            (self.start_dt, self.end_dt) = (datetime.strptime(i, "%Y-%m-%d") for i in self.period.split(' '))
            df = df[(df['date'] >= self.start_dt.date()) & (df['date'] <= self.end_dt.date())].copy()

        if df.empty:
            raise ValueError('Empty DataFrame.')

        if self.return_base == 'pre_close':
            df['return'] = df['close'].diff()
        elif self.return_base == 'open':
            df['return'] = df['close'] - df['open']
        else:
            raise AttributeError(
                f'Wrong parameter passed: return_base. current: {self.return_base}, '
                f'must be pre_close or open.'
            )

        self.df = df

        # initiate some params
        self.collect_perfs = {}

        # initiate redis_mq
        self.rds_handler = rh.RedisHandle(self.mq_name)

    def backtest(self):
        self.df.fillna(0, inplace=True)
        """
        根据每日是否清仓，分为两种情况：
        如果每日有清仓时间，那么信号和清仓后的第一根bar的收益需要进行处理
        默认最后一根bar不产生交易信号，也没有持仓
        """
        if self.trade_flag != 0:
            # 每日有清仓时间， 并且计算收益类型不是基于当前bar的开盘价
            self.df['time'] = self.df.index.time
            # 标记清仓信号-888
            self.df['signal'] = np.where(self.df['time'].isin(self.trade_flag), -888, self.df['signal'])
            # 收盘清仓后的第一个bar的收益应该是基于当时的开盘价，而非上一交易日的收盘价
            if self.return_base != 'open':
                self.df['return'] = np.where(
                    self.df['time'].shift().isin(self.trade_flag),
                    self.df['close'] - self.df['open'],
                    self.df['return']
                )
            else:
                pass
        else:
            # 没有清仓时间，不做处理
            pass
        # 在对0信号持有情形进行补0操作之前对原始开仓数据进行一次复制， 保证开仓信号是正确的
        self.df['orig_sig'] = self.df['signal'].copy().replace(to_replace=-888, value=0)
        """
        在以上基础上，再对信号0是持仓还是清仓的两种情况进行处理
        """
        if self.action_on_0 == 'hold':
            # 0信号持有，则将所有0信号都以之前不为0信号填充
            self.df['real_sig'] = self.df['signal'].replace(to_replace=0, method='pad').replace(to_replace=-888,
                                                                                                value=0)
        elif self.action_on_0 == 'close':
            self.df['real_sig'] = self.df['signal'].replace(to_replace=-888, value=0)
        else:
            raise AttributeError("Param action can only be 'close' or 'hold'.")
        """
        在以上基础上，再对止损点进行处理
        """
        # 先还原signal
        self.df['signal'] = self.df['signal'].replace(to_replace=-888, value=0)
        # 在signal上标记止损点
        self.df['signal'] = np.where(
            self.df['real_sig'] * self.df['return'] * self.pos_ <= -self.stop_loss,
            -999,
            self.df['signal']  # 这里必须是self.df['signal'] 不可以是self.df['real_sig']
        )
        # 先把止损点盈亏记录到策略收益中
        self.df['factor_return'] = np.where(
            self.df['signal'] == -999,
            -self.stop_loss,
            0
        )
        """
        哪里有交易，哪里就有手续费，即信号变化的地方，产生手续费
        手续费需要规避的情况是：开仓时连续的同向持仓需要避免多次计算手续费。
        """
        # 开仓信号
        self.df['op_pos'] = np.where(
            (self.df['orig_sig'].shift() != self.df['orig_sig']) & (abs(self.df['orig_sig']) != 0),  # 使用最初的开仓信号
            1,
            0
        )
        # 平仓信号
        # 平仓信号第一部分：止损平仓
        self.df['cl_pos'] = np.where(
            self.df['signal'] == -999,
            1,
            0
        )
        # 此时当所有需要用到止损标记的地方都结束之后，还需要再对0信号进行一遍填充，因为real_sig经过止损操作之后已经不能代表准确的持仓信号
        # 例如原先信号是1 0 0 1 0 ，补齐之后变成 1 1 1 1 1 ，止损后变成 1 -999 1 1 1 ，但实际应该是 1 -999 0 1 1， 最终需要的是 1 0 0 1 1
        # 所以在这里需要对signal再次进行信号0的处理：
        if self.action_on_0 == 'hold':
            self.df['signal'] = self.df['signal'].replace(to_replace=0, method='pad').replace(to_replace=-999, value=0)
        elif self.action_on_0 == 'close':
            self.df['signal'] = self.df['signal'].replace(to_replace=-999, value=0)
        else:
            pass
        # 然后再计算平仓信号的第二部分， 0信号平仓 + 反转平仓
        self.df['cl_pos'] += np.where(
            (abs(self.df['signal'].shift()) == 1) & (self.df['signal'] == 0),
            1,
            0
        ) + np.where(
            self.df['signal'].shift() * self.df['signal'] == -1,
            1,
            0
        )
        # 统计开平仓次数
        self.collect_perfs['total_open'] = self.df['op_pos'].sum()
        self.collect_perfs['total_close'] = self.df['cl_pos'].sum()
        # 计算交易摩擦成本
        self.df['friction_loss'] = (self.df['op_pos'] + self.df['cl_pos']) * self.friction_loss * self.pos_
        # 计算收益，采取累加的形式计算收益。
        self.df['factor_return'] += self.df['signal'] * self.df['return'] * self.pos_
        self.df['factor_return_price'] = self.df['factor_return'] - self.df['friction_loss']
        self.df['factor_return'] = self.df['factor_return_price'] / self.df['close'].shift()
        self.df['factor_return_cumsum'] = self.df['factor_return_price'].cumsum() / self.df.iloc[0]['close']

        # 基准收益没有手续费和止损等处理，只考虑了仓位
        self.df['benchmark_return_price'] = self.df['return'] * self.pos_
        self.df['benchmark_return'] = self.df['benchmark_return_price'] / self.df['close'].shift()
        self.df['benchmark_return_cumsum'] = self.df['benchmark_return_price'].cumsum() / self.df.iloc[0]['close']

        self.collect_perfs['total_win'] = len(self.df[self.df['factor_return'] > 0]['factor_return'])
        self.collect_perfs['total_lose'] = len(self.df[self.df['factor_return'] < 0]['factor_return'])
        self.df = self.df[[
            'open', 'close', 'factor', 'signal', 'return', 'date',
            'factor_return', 'op_pos', 'cl_pos', 'friction_loss',
            'factor_return_cumsum', 'benchmark_return', 'benchmark_return_cumsum'
        ]]
        id = hashlib.md5((
                f"{self.settings.get('instrument')}"
                f"{self.settings.get('factors')}"
                f"{self.settings.get('params')}"
                f"{self.settings.get('sig_shift')}").encode('utf8')).hexdigest()
        res_dict = {
            'data': pickle.dumps(self.df),
            **self.settings,
            **self.collect_perfs,
            'test_id': id,
        }

        result = pickle.dumps(res_dict)
        self.rds_handler.push_msg(result)
        logger.info(
            f"\n{pd.DataFrame.from_dict(self.settings, orient='index', columns=[id]).T.to_markdown()} \nDone!"
        )


# if __name__ == "__main__":
#     md_df = pd.read_parquet("../../../data/THS/raw/cn/futures/data_sample/IF_main_2019-2021.parquet")
#     factor = pd.read_parquet("../../../data/1.parquet")
#     settings = {
#         'instrument': 'IF',
#         'factors': 'nmf',
#         'params': 960,
#         'sig_shift': 1,
#         'timeframe': 'T1',
#         'datasource': 'THS',
#         'friction_loss': 0.4,
#         'return_base': 'pre_close',
#         'position_pctg': 1,
#         'stop_loss': 100,
#         'sig_0_action': 'close',
#         'day_close': "15:30",
#         "period": "2019-01-01 2021-01-01"
#     }
#     backtest = BackTest(
#         md_df,
#         factor,
#         settings=settings
#     )
#     backtest.backtest()