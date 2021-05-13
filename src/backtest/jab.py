"""
backtest logic
another revision.
"""

import pandas as pd
import numpy as np
import os
import logging
import inspect
from datetime import datetime, time
from empyrical import alpha_beta, sharpe_ratio, max_drawdown, annual_return, tail_ratio
import src.factor.factor as ft
import src.visualization.rich_visual as rv

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

fp = os.path.dirname(__file__)

logpath = os.path.join(fp, "../../docs/backtest/backtest_log.txt")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
filing = logging.FileHandler(filename=logpath)
streaming = logging.StreamHandler()
formatter = logging.Formatter(fmt="%(asctime)s - %(filename)s - %(levelname)s - %(message)s",
                              datefmt="%Y-%m-%d %H:%M:%S")
filing.setFormatter(formatter)
streaming.setFormatter(formatter)
logger.addHandler(filing)
logger.addHandler(streaming)


class BackTester:

    def __init__(
            self,
            id: list,
            data_source: str,
            timeframe: str,
            test_date: list,
            data_type: str,
            day_close: [list, None],
            stop_loss: float,
            sig_0_action: str,
            position_pctg: float,
            transaction_fee_pctg: float,
    ):
        """
        Test it before use it, bro!
        Args:
            id: target symbol or contract code
            data_source: ~
            timeframe: ~ T5 T1 etc...
            test_date: pass in sth like [2017, 2021]
            data_type: ~
            stop_loss:  stop loss percentage on each bar.
            sig_0_action:  action to take on signal 0, pass 'close' or 'hold'
            position_pctg:  how much exposure on your position, this will affect both benchmark position and strategy position
        """
        self.trans_fee_pctg = transaction_fee_pctg
        self.data_source = data_source.upper()
        self.timeframe = timeframe
        self.start_dt = datetime.strptime(test_date[0], "%Y-%m-%d")
        self.end_dt = datetime.strptime(test_date[1], "%Y-%m-%d")
        self.id = id
        self.data_type = data_type
        self.fac = ft.FactorX(
            self.id,
            self.timeframe,
            self.data_source,
            test_date[0],
            test_date[1],
        )

        # # load data from clickhouse
        # df = self.fac.main_df.copy()
        # or load from parquet files
        df = pd.read_parquet("../data/futures/raw/intraday_data/index300_main_430-510.parquet")
        df.index = pd.to_datetime(df.index)
        df.sort_index(ascending=True, inplace=True)
        df = df[(df.index.date >= self.start_dt.date()) & (df.index.date <= self.end_dt.date())].copy()

        if df.empty:
            raise ValueError('Empty DataFrame.')
        df['return_close'] = round((df['close'] / df['close'].shift() - 1).fillna(0), 5)
        self.fac.reset_df(df)
        self.source_df = df.copy()
        self.df = df.copy()

        # initiate params
        self.pnl = 0
        self.pos_ = position_pctg

        self.stop_loss = stop_loss
        self.action_on_0 = sig_0_action

        self.winner, self.loser = 0, 0

        self.trade_flag = [time.fromisoformat(t) for t in day_close] if day_close is not None else 0

        self.func, self.neut_func, self.neut = None, None, None

    def reload_source(self):
        self.df = self.source_df
        self.pnl = 0
        self.winner, self.loser = 0, 0

    def make_bias_signal(self, valve, w_s, func, sig_shift, kwargs):
        """

        Args:
            valve: greater than valve to be a valid signal else 0
            func: strategy function
            sig_shift: signal shift
            **kwargs:
        """
        factors_ls = [func[i](**(kwargs[i])).shift(sig_shift[i]) for i in range(len(func))]
        self.bias_factor_df = pd.DataFrame([v['factor'].rename(func[k].__name__) for k, v in enumerate(factors_ls)]).T
        self.bias_factor_df['return_close'] = self.df['return_close']
        self.df['signal'] = self.fac.factor_compound([i['signal'] for i in factors_ls], w=w_s, valve=valve)
        self.signal_params = kwargs
        self.func = func
        self.sig_shift = sig_shift

        if self.neut is not None:
            self.df['neut'] = self.neut
            self.df['signal'] = np.where(self.df['neut'] == 1, self.df['signal'], 0)
        else:
            self.neut_factor_df = None

    def make_neut_signal(self, valve, w_s, func, sig_shift, kwargs):
        factors_ls = [func[i](**(kwargs[i])).shift(sig_shift[i]) for i in range(len(func))]
        self.neut_factor_df = pd.DataFrame([v['factor'].rename(func[k].__name__) for k, v in enumerate(factors_ls)]).T
        self.neut_factor_df['return_close'] = self.df['return_close']
        neut = self.fac.factor_compound([i['signal'] for i in factors_ls], w=w_s, valve=valve)
        self.neut = neut
        self.neut_func = func

    def backtester(self):
        self.df.fillna(0, inplace=True)
        self.df['signal'] = self.df['signal'].fillna(0).astype('int64')
        self.signal_ratio = np.count_nonzero(self.df['signal']) / len(self.df['signal'])
        """
        根据每日是否清仓，分为两种情况：
        如果每日有清仓时间，那么信号和清仓后的第一根bar的收益需要进行处理
        默认最后一根bar不产生交易信号，也没有持仓
        """
        if self.trade_flag != 0:
            # 每日有清仓时间
            self.df['time'] = self.df.index.time
            # 标记清仓信号-888
            self.df['signal'] = np.where(self.df['time'].isin(self.trade_flag), -888, self.df['signal'])
            # 收盘清仓后的第一个bar的收益应该是基于当时的开盘价，而非上一交易日的收盘价
            self.df['return_close'] = np.where(
                self.df['time'].shift().isin(self.trade_flag),
                round(((self.df['close'] - self.df['open']) / self.df['open']).fillna(0), 5),
                self.df['return_close']
            )
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
            self.df['real_sig'] * self.df['return_close'] * self.pos_ <= -self.stop_loss,
            -999,
            self.df['signal']  # 这里必须是self.df['signal'] 不可以是self.df['real_sig']
        )
        # 先把止损点盈亏记录到策略收益中
        self.df['strat_return'] = np.where(
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
            (self.df['orig_sig'].shift() != self.df['orig_sig']) & (abs(self.df['orig_sig']) == 1),  # 使用最初的开仓信号
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
        self.open_t = self.df['op_pos'].sum()
        self.close_t = self.df['cl_pos'].sum()
        # 计算手续费
        self.df['transfee'] = (self.df['op_pos'] + self.df['cl_pos']) * self.trans_fee_pctg * self.pos_
        # 计算收益，采取累加的形式计算收益。
        self.df['strat_return'] += self.df['signal'] * self.df['return_close'] * self.pos_
        self.df['strat_return'] = self.df['strat_return'] - self.df['transfee']
        self.df['strat_ret_cumsum'] = self.df['strat_return'].cumsum()

        # 基准收益没有手续费和止损等处理，只考虑了仓位
        self.df['bench_return'] = self.df['return_close'] * self.pos_
        self.df['bench_ret_cumsum'] = self.df['bench_return'].cumsum()

        self.winner = len(self.df[self.df['strat_return'] > 0]['strat_return'])
        self.loser = len(self.df[self.df['strat_return'] < 0]['strat_return'])
        self.pnl = self.df['strat_return'].sum()

    def describer(self):
        tot_cnt = len(self.df)

        missed = self.df[
            (np.sign(self.df['return_close']) != self.df['signal']) &
            (self.df['signal'] == 0)
            ]['strat_return'].describe()
        wrong = self.df[
            (np.sign(self.df['return_close']) == self.df['signal'] * (-1)) &
            (self.df['signal'] != 0)
            ]['strat_return'].describe()
        jackpot = self.df[
            (np.sign(self.df['return_close']) == self.df['signal'])
        ]['strat_return'].describe()

        desc = pd.DataFrame([jackpot, wrong, missed], index=['jackpot', 'wrong', 'missed']).T.to_markdown()

        trans_fee_tot = self.df['transfee'].sum()

        return_ret, return_bench = {}, {}
        self.df['date'] = self.df.index.date
        for (k, v) in self.df.groupby('date'):
            return_ret[k] = v['strat_return'].sum()
            return_bench[k] = v['bench_return'].sum()
        rret = pd.Series(list(return_ret.values()), index=list(return_ret.keys()), name='rret')
        rbench = pd.Series(list(return_bench.values()), index=list(return_bench.keys()), name='rbench')
        (alpha, beta) = alpha_beta(rret, rbench, period='daily')
        sharpe = sharpe_ratio(rret, period='daily')
        max_down = max_drawdown(rret)
        ann_return_strat = annual_return(rret, period='daily')
        ann_return_bench = annual_return(rbench, period='daily')
        t_r = tail_ratio(rret)

        returns = f"Strategy Return: {round(self.pnl * 100, 2)}% | " \
                  f"Strategy Annualized Return: {round(ann_return_strat * 100, 2)}%. \n" \
                  f"BenchMark return: {round(self.df['bench_return'].sum() * 100, 2)}% | " \
                  f"BenchMark Annualized Return: {round(ann_return_bench * 100, 2)}%.\n"

        desc_ = f"Strategy: {self.func} \n" \
                f"Transaction Fee Percentage: {self.trans_fee_pctg}\n" \
                f"Intraday Closing Time: {self.trade_flag}\n" \
                f"Params: {self.signal_params}\n" \
                f"Test Period: {self.start_dt} - {self.end_dt}\n" \
                f"-- {self.id} --\n" \
                f"-- {self.timeframe} -- \n" \
                f"-- Position: {self.pos_} --\n" \
                f"-- Barly Stoploss: {self.stop_loss} --\n" \
                f"-- Action on Sig0: {self.action_on_0} --\n" \
                f"-- Signal Shift: {self.sig_shift} --\n" \
                f"Transaction Fee Total: {round(trans_fee_tot * 100, 2)}%\n" \
                f"Signal Ratio: {round(self.signal_ratio * 100, 2)}%\n" \
                f"Open Position: {self.open_t} times; Close Position: {self.close_t} times\n" \
                f"Sharpe Ratio: {round(sharpe, 2)} \n" \
                f"Tail Ratio: {round(t_r, 2)}\n" \
                f"Alpha: {round(alpha * 100, 2)}% | Beta: {round(beta * 100, 2)}% \n" \
                f"Max Drawdown: {round(max_down * 100, 2)}% \n" \
                f"Max Daily Drawdown: {round(rret.min() * 100, 2)}% \n" \
                f"Total Win: {self.winner} | Total Loss: {self.loser} | " \
                f"W/L Ratio: {round(self.winner / self.loser, 2) if self.loser != 0 else 0}\n"

        source_code = "\n\n".join(
            [inspect.getsource(f) for f in self.func]
        ) if self.func is not None else " "
        source_code_neut = "\n\n".join(
            [inspect.getsource(f) for f in self.neut_func]
        ) if self.neut_func is not None else " "

        t_stamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        logger.info(f"-- {t_stamp} --\n{desc_}{returns}\n")

        # 所有回测都值得记录
        path_ = os.path.join(
            fp,
            f"../../docs/backtest/"
            f"{self.id}-{self.timeframe}-Sharpe{round(sharpe, 2)}-{datetime.now().strftime('%y%m%d-%H:%M:%S')}/"
        )
        os.mkdir(path_)

        plot = self.df[['close', 'strat_ret_cumsum', 'bench_ret_cumsum']].plot(
            figsize=(16, 9), secondary_y='close'
        )
        fig = plot.get_figure()
        fig_path = os.path.join(
            path_, f"return_curve.png"
        )
        fig.savefig(fig_path)

        rec_path = os.path.join(
            path_, "trade_record.csv"
        )
        self.df.to_csv(rec_path)

        desc_path = os.path.join(
            path_, "desc.txt"
        )
        with open(desc_path, mode="w+", encoding='utf8') as f:
            f.write(
                desc_ +
                returns +
                '\n' +
                f'\nTotal Bars: {tot_cnt} \n' +
                '\nStatistics Desc: \n' +
                desc +
                '\n* NOTE: THIS DESCRIPTION DIFFERS FROM W/L RATIO ABOVE '
                'BECAUSE ONLY SIGNAL DIRECTION CORRECTNESS IS CONSIDERED HERE.\n' +
                '\n\nBias_factors: \n' + source_code +
                '\nNeut_factors: \n' + source_code_neut
            )

        # 但只有高夏普低回撤的回测才配拥有高级可视化
        if (sharpe > 1.5) & (max_down >= -0.2 * self.pos_):
            rich_visual_path = os.path.join(
                path_, "rich_visual.html"
            )
            kline = rv.draw_kline_with_yield_and_signal(self.df)
            scatters_fr = rv.draw_factor_return_eval(self.bias_factor_df)
            scatters_ff = rv.draw_factor_eval(self.bias_factor_df)
            res_charts = [kline, *scatters_fr, *scatters_ff]
            if self.neut_factor_df is not None:
                sca_neut_fr = rv.draw_factor_return_eval(self.neut_factor_df)
                sca_neut_ff = rv.draw_factor_eval(self.neut_factor_df)
                res_charts += [*sca_neut_fr, *sca_neut_ff]
            rv.form_page(res_charts, rich_visual_path)

    def multi_backtester(self, w_s, valve, func, sig_shift, params: list):
        for param_set in params:
            self.make_bias_signal(
                valve=valve,
                w_s=w_s,
                func=func,
                sig_shift=sig_shift,
                kwargs=param_set,
            )
            self.backtester()
            self.describer()
            self.reload_source()

    def test_signal_corr(self):
        cor_ = np.sign(self.df['close'] / self.df['close'].shift() - 1).corr(self.df['signal'])
        sig_ratio = np.count_nonzero(self.df['signal']) / len(self.df['signal'])
        result = f"Signal Test: {self.id}\n" \
                 f"-- Test Period: {self.start_dt} - {self.end_dt}\n" \
                 f"-- Factor: {self.func}\n" \
                 f"-- Signal Params: {self.signal_params}\n" \
                 f"-- Timeframe: {self.timeframe}\n" \
                 f"-- Correlation: {cor_}\n" \
                 f"-- Signal Ratio: {round(sig_ratio * 100, 2)}%\n"
        logger.info(result)

    def multi_test_signal_corr(self, w_s, valve, func, sig_shift, params):
        for param in params:
            self.make_bias_signal(
                valve=valve,
                w_s=w_s,
                func=func,
                sig_shift=sig_shift,
                kwargs=param
            )
            self.test_signal_corr()