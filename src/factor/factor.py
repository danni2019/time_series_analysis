"""
author: muzexlxl
email: muzexlxl@foxmail.com

time series factors
bias: -1 0 1
neut: 1, 0
"""

import pandas as pd
import numpy as np
import src.data.clickhouse_control as cc


class FactorX:

    def __init__(self, init_: [dict, pd.DataFrame]):
        if type(init_) == dict:
            # id: list, timeframe: str, data_source: str, start: str, end: str
            id, timeframe, data_source, start, end = init_.get('id'), \
                                                     init_.get('timeframe'), \
                                                     init_.get('data_source'), \
                                                     init_.get('start'), \
                                                     init_.get('end')
            self.db_conn = cc.ClickHouse(data_source)
            self.id = id
            if self.id[0] == 'symbol':
                self.database = self.db_conn.db_conf.db_processed
                self.data_table = self.db_conn.db_conf.processed_trade_data_main
            elif self.id[0] == 'code':
                self.database = self.db_conn.db_conf.db_raw
                self.data_table = self.db_conn.db_conf.raw_trade_data
            else:
                raise AttributeError(f'Wrong id type: {self.id[0]}')
            self.timeframe = timeframe
            self.data_source = data_source
            self.main_df = self.data_reader(start, end)
        elif isinstance(init_, pd.DataFrame):
            self.main_df = init_
        else:
            raise TypeError(f"Wrong initial data type passed: expect dict or DataFrame, got {type(init_)} instead.")

    def data_reader(self, start_date, end_date):
        sql_ = f"select `code`, `symbol`, `datetime`, `open`, `close`, " \
               f"`high`, `low`, `turnover`, `volume`, `open_interest` from " \
               f"{self.database}.{self.data_table} where `{self.id[0]}` = '{self.id[1]}' and " \
               f"`timeframe` = '{self.timeframe}' and `data_source` = '{self.data_source}' and " \
               f"`datetime` >= '{start_date}' and `datetime` <= '{end_date}'"
        df = self.db_conn.reader_to_dataframe(sql_)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['date'] = df['datetime'].dt.strftime("%Y-%m-%d")
        return df.set_index('datetime')

    def reset_df(self, df: pd.DataFrame):
        self.main_df = df

    def factor_tmom_neut_01(self, w):
        """adx indicator"""
        source_df = self.main_df.copy()
        source_df['up'] = source_df['high'] - source_df['high'].shift()
        source_df['down'] = source_df['low'].shift() - source_df['low']
        source_df['dm+'] = np.where(
            (source_df['up'] > source_df['down']) & (source_df['down'] > 0), source_df['up'], 0
        )
        source_df['dm-'] = np.where(
            (source_df['down'] > source_df['up']) & (source_df['up'] > 0), source_df['down'], 0
        )
        source_df['hl'] = source_df['high'] - source_df['low']
        source_df['hc'] = abs(source_df['high'] - source_df['close'])
        source_df['lc'] = abs(source_df['low'] - source_df['close'])
        source_df['atr'] = source_df[['hl', 'hc', 'lc']].max(axis=1).rolling(w).mean()
        source_df['di+'] = (source_df['dm+'].rolling(w).mean() / source_df['atr']) * 100
        source_df['di-'] = (source_df['dm-'].rolling(w).mean() / source_df['atr']) * 100
        source_df['dx'] = ((source_df['di+'] - source_df['di-']) / (source_df['di+'] + source_df['di-'])) * 100
        source_df['adx'] = source_df['dx'].rolling(w).mean()
        source_df['factor'] = np.where(source_df['adx'] > 25, source_df['adx'], 0)
        source_df['signal'] = np.where(
            (source_df['factor'] / source_df['factor'].shift()).fillna(0) > 1,
            1,
            0
        )
        return source_df[['factor', 'signal']]

    def factor_tmom_bias_01(self, w):
        source_df = self.main_df.copy()
        source_df['return_close'] = source_df['close'].diff() / source_df['close'].shift()
        ls = source_df['return_close'].rolling(w).apply(
            lambda x: pd.Series([(i/abs(i)) if abs(i) > 0 else 0 for i in x.cumsum()[::-5]]).mean()
        )
        source_df['factor'] = [i if abs(i) > 0.5 else 0 for i in ls]
        source_df['signal'] = np.sign(source_df['factor'])
        return source_df[['factor', 'signal']]


    @staticmethod
    def factor_compound(factors, w: [int, None], valve: int):
        compounded_factor = pd.DataFrame(factors).T.mean(axis=1)
        if w is not None:
            compounded_factor = compounded_factor.rolling(w).mean().apply(
                lambda x: np.where(abs(x) > valve, np.sign(x), 0)
            )
        else:
            compounded_factor = compounded_factor.apply(lambda x: x/abs(x) if abs(x) >= valve else 0)
        return compounded_factor

