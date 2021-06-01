import os
import timeit
from typing import Union
import pandas as pd
import pickle
import multiprocessing
from src.backtest.func_modules import backtest, statistics
import src.data.redis_handle as rh
from src.backtest.test_config import test_settings

# create a redis conn to set backtest status flag to 1, stating in process of a backtest.
rds = rh.RedisHandle()


class Task:
    def __init__(
        self,
        settings: Union[dict, None] = None
    ):
        if settings is None:
            self.settings = test_settings
        else:
            self.settings = settings
        self.tests = self.settings.get('tests', [])
        self.test_name = self.settings.get('test_name', 'default')

    def backtest_runner(self):
        for p in self.tests:
            symbol = p.get('instrument')
            md_file = p.get('md')
            factor_name = p.get('factors')
            self.sig_shift = p.get('sig_shift')
            md_path = os.path.join(
                os.path.dirname(__file__),
                f"../../data/futures/raw/intraday_data/{md_file}.parquet"
            )
            factor_path = os.path.join(
                os.path.dirname(__file__),
                f"../../factor/{symbol}/{factor_name}.parquet"
            )
            sig_path = os.path.join(
                os.path.dirname(__file__),
                f"../../signal/{symbol}/{factor_name}.parquet"
            )

            md = pd.read_parquet(md_path)
            fac_df = pd.read_parquet(factor_path).shift(self.sig_shift)
            sig_df = pd.read_parquet(sig_path).shift(self.sig_shift)
            p['test_name'] = self.test_name
            bt = backtest.BackTest(
                meta_data=md,
                factor_data=fac_df,
                signal_data=sig_df,
                settings=p
            )
            bt.backtest()
        # after all backtest finishes, send a finishing msg into queue
        rds.push_msg(
            pickle.dumps('elfin')
        )

    def describe_runner(self):
        desc = statistics.Desc()
        desc.run_desc()

    def main_run(self):
        start_ = timeit.default_timer()
        pool = multiprocessing.Pool(2)
        for i in [self.describe_runner, self.backtest_runner]:
            pool.apply_async(func=i)
        pool.close()
        pool.join()
        pool.terminate()
        print("time consumption: ", timeit.default_timer() - start_)



if __name__ == "__main__":
    task = Task()
    task.main_run()
