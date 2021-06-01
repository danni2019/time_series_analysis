import pandas as pd
import numpy as np
import pickle
import asyncio
from datetime import datetime
import os
import logging
from empyrical import alpha_beta, sharpe_ratio, max_drawdown, annual_return, tail_ratio

import src.data.redis_handle as rh
import src.visualization.plotting as pl

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


class Desc:

    def __init__(self, mq_name: str = 'default'):
        logger.info("Initiating Stats Description.")
        self.rds_handler = rh.RedisHandle(mq_name)
        self.compare_df, self.desc_df = pd.DataFrame(), pd.DataFrame()
        self.base_path = os.path.dirname(__file__)
        self.time_base = datetime.now().strftime("%Y%m%d_%H_%M_%S")

        self.loop = asyncio.get_event_loop()

    def task_assigner(self):
        msg_ = self.rds_handler.get_msg()
        return msg_ is not None and pickle.loads(msg_) or None

    async def describer(self, msg):
        df: pd.DataFrame = pickle.loads(msg.get('data'))
        total_cnt = len(df)
        signal_ratio = np.count_nonzero(df['signal']) / len(df['signal'])

        missed = len(df[
            (np.sign(df['return']) != df['signal']) & (df['signal'] == 0)
            ])

        wrong_avg = df[
            (np.sign(df['return']) == df['signal'] * (-1)) & (df['signal'] != 0)
            ]['factor_return'].mean()
        wrong_median = df[
            (np.sign(df['return']) == df['signal'] * (-1)) & (df['signal'] != 0)
            ]['factor_return'].median()
        wrong_total = len(
            df[
                (np.sign(df['return']) == df['signal'] * (-1)) & (df['signal'] != 0)
            ]
        )

        right_avg = df[
            (np.sign(df['return']) == df['signal'])
        ]['factor_return'].mean()
        right_median = df[
            (np.sign(df['return']) == df['signal'])
        ]['factor_return'].median()
        right_total = len(
            df[
                (np.sign(df['return']) == df['signal'])
            ]
        )

        frictions = (df['friction_loss'].sum() / df.iloc[0]['close']) * 100

        return_ret, return_bench = {}, {}

        for (k, v) in df.groupby('date'):
            return_ret[k] = v['factor_return'].sum()
            return_bench[k] = v['benchmark_return'].sum()
        rret = pd.Series(list(return_ret.values()), index=list(return_ret.keys()), name='rret')
        rbench = pd.Series(list(return_bench.values()), index=list(return_bench.keys()), name='rbench')
        (alpha, beta) = alpha_beta(rret, rbench, period='daily')
        sharpe = sharpe_ratio(rret, period='daily')
        max_down = max_drawdown(rret)
        ann_return_strat = annual_return(rret, period='daily')
        ann_return_bench = annual_return(rbench, period='daily')
        t_r = tail_ratio(rret)

        timeframe = msg.get('timeframe', None)

        desc = {
            'TestPeriod': msg.get('period', None),
            'Instrument': msg.get('instrument'),
            'Factors': msg.get('factors'),
            'SignalShift': msg.get('sig_shift'),
            'Timeframe': timeframe,
            'Datasource': msg.get('datasource', None),
            'FrictionLoss': msg.get('friction_loss'),
            'ReturnBase': msg.get('return_base'),
            'Position': f"{round(msg.get('position_pctg') * 100, 2)} %",
            'StopLoss': msg.get('stop_loss') < 100 and msg.get('stop_loss') or None,
            'Sig0Action': msg.get('sig_0_action'),
            'DayClose': msg.get('day_close'),
            'TotalOpen': msg.get('total_open'),
            'TotalClose': msg.get('total_close'),
            'TotalBars': total_cnt,
            'TurnoverRate': f"{round(((msg.get('total_open') + msg.get('total_close')) / total_cnt) * 100, 2)}%",
            'TotalWin': msg.get('total_win'),
            'TotalLose': msg.get('total_lose'),
            "W/L Ratio": msg.get('total_lose') != 0 and round(msg.get('total_win') / msg.get('total_lose'), 2) or msg.get(
                'total_win'),
            "RightTotal": right_total,
            "RightAvgReturn": round(right_avg, 5),
            "RightMedianReturn": round(right_median, 5),
            "WrongTotal": wrong_total,
            "WrongAvgReturn": round(wrong_avg, 5),
            "WrongMedianReturn": round(wrong_median, 5),
            "MissedTotal": missed,
            'StrategyReturn': f"{round(df['factor_return'].sum() * 100, 2)}%",
            'StrategyAnnualReturn': f"{round(ann_return_strat * 100, 2)}%",
            'BenchmarkReturn': f"{round(df['benchmark_return'].sum() * 100, 2)}%",
            'BenchmarkAnnualReturn': f"{round(ann_return_bench * 100, 2)}%",
            'TotalFrictionLoss': f"{round(frictions, 2)}%",
            'SignalRatio': f"{round(signal_ratio * 100, 2)}%",
            'SharpeRatio': round(sharpe, 2),
            "TailRatio": round(t_r, 2),
            "Alpha": f"{round(alpha * 100, 2)}%",
            "Beta": f"{round(beta * 100, 2)}%",
            "MaxDrawdown": f"{round(max_down * 100, 2)}%",
            "MaxDailyDrawdown": f"{round(rret.min() * 100, 2)}%",
        }

        if 'close' not in self.compare_df.columns:
            self.compare_df['close'] = df['close']
            self.compare_df['benchmark'] = df['benchmark_return_cumsum']

        if timeframe[0] == "T":
            self.compare_df[
                f"{msg.get('instrument')}-{msg.get('factors')}"
            ] = df['factor_return_cumsum']
        else:
            df.index = pd.to_datetime((df.index.astype(str) + " " + "15:00:00"))
            self.compare_df[
                f"{msg.get('instrument')}-{msg.get('factors')}"
            ] = df['factor_return_cumsum']
            self.compare_df[
                f"{msg.get('instrument')}-{msg.get('factors')}"
            ] = self.compare_df[
                f"{msg.get('instrument')}-{msg.get('factors')}"
            ].fillna(0).replace(to_replace=0, method='pad')

        self.desc_df[msg.get('test_id')] = pd.DataFrame.from_dict(desc, orient='index', columns=['desc'])['desc']

        # 将回测结果的元数据存入路径

        # 同一批次的回测存入同一个文件夹中
        self.f_path = path_ = os.path.join(
            self.base_path,
            f"../../../docs/backtest/"
            f"{msg.get('instrument')}-"
            f"{msg.get('test_name')}-"
            f"{self.time_base}"
            f"/"
        )
        try:
            os.mkdir(path_)
        except FileExistsError:
            pass
        else:
            pass

        # 批次文件夹内，再按照test_id存入不同的文件夹下
        meta_path = os.path.join(path_, f"./{msg.get('test_id')}/")

        try:
            os.mkdir(meta_path)
        except FileExistsError:
            pass
        else:
            pass

        df['drawdown_'] = np.where(
            df['factor_return'] < 0,
            df['factor_return'],
            0,
        )
        # 画单个的收益曲线
        fig_path = os.path.join(
            meta_path, f"return_curve.png"
        )
        pl.draw_overlay_lines(
            data=df[['close', 'factor_return_cumsum', 'benchmark_return_cumsum']],
            secondary_y='close',
            filename=fig_path,
        )

        # 画收益对比回撤的图
        fig_path = os.path.join(
            meta_path, f"drawdown_map.png"
        )
        pl.draw_overlay_lines(
            data=df[['factor_return_cumsum', 'drawdown_']],
            secondary_y='drawdown_',
            filename=fig_path,
        )

        # 画因子-收益图
        fig_path = os.path.join(
            meta_path, f"factor_yield.png"
        )
        pl.draw_scatter(
            data=df[['factor', 'factor_return']],
            x='factor',
            y='factor_return',
            filename=fig_path,
        )

        # 画信号-收益图
        fig_path = os.path.join(
            meta_path, f"signal_yield.png"
        )
        pl.draw_scatter(
            data=df[['signal', 'factor_return']],
            x='signal',
            y='factor_return',
            filename=fig_path,
        )

        # 将原始的交易记录保存到csv
        rec_path = os.path.join(
            meta_path, "trade_record.csv"
        )
        df.to_csv(rec_path)

        # 描述文件
        desc_text = f"Description: \n" \
                    f"{pd.DataFrame.from_dict(desc, orient='index', columns=[msg.get('test_id')]).to_markdown()}"
        with open(f"{meta_path}desc.txt", mode='w+', encoding='utf8') as f:
            f.write(desc_text)

    async def task_ready(self):
        task_list = []
        while True:
            msg = self.task_assigner()
            if msg is not None and msg != 'elfin':
                logger.info("Fetched 1 Message.")
                task = self.loop.create_task(self.describer(msg))
                task_list.append(task)
            elif msg == 'elfin':
                break
            else:
                pass
        await asyncio.gather(*task_list)

    def run_desc(self):
        self.loop.run_until_complete(self.task_ready())
        # 画综合收益图
        fig_path = os.path.join(self.f_path, "collective_return_curve.png")
        pl.draw_overlay_lines(
            data=self.compare_df,
            secondary_y='close',
            filename=fig_path,
        )
        # 存入综合描述文件
        with open(f"{self.f_path}collective_description.txt", mode='w+', encoding='utf8') as f:
            f.write(
                self.desc_df.to_markdown()
            )


if __name__ == "__main__":
    ass = Desc()
    ass.run_desc()
    # ass.rds_handler.del_key('default')