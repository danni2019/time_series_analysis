"""
time series momentum factors
Only consolidated factors are saved in this file.
"""
import os
import inspect
from datetime import datetime
import configparser

import pandas as pd

fp_conf = os.path.dirname(__file__)

config = configparser.ConfigParser()
config.read(os.path.join(fp_conf, "../../conf.ini"))


def record_source(func):
    source_code = inspect.getsource(func)
    with open(
            os.path.join(fp_conf, "../../src/features/factors/factor_tmom.py"),
            mode='a+',
            encoding='utf8'
    ) as f:
        f.write(f"# Record time: {datetime.now().strftime('%Y%m%d %H:%M:%S')}\n")
        f.write(source_code)
        f.write("\n\n")


def save_factor(df: pd.DataFrame, fp: str, factor_name: str):
    factor_path = os.path.join(fp, f"{factor_name}.parquet")
    try:
        origin_df = pd.read_parquet(factor_path)
    except FileNotFoundError:
        df.to_parquet(factor_path)
    else:
        origin_df = pd.concat([origin_df, df[df['factor'].index > max(origin_df['factor'].index)][['factor']]])
        origin_df.to_parquet(factor_path)


def save_signal(df: pd.DataFrame, fp: str, signal_name: str):
    signal_path = os.path.join(fp, f"{signal_name}.parquet")
    try:
        origin_df = pd.read_parquet(signal_path)
    except FileNotFoundError:
        df.to_parquet(signal_path)
    else:
        origin_df = pd.concat([origin_df, df[df['signal'].index > max(origin_df['signal'].index)][['signal']]])
        origin_df.to_parquet(signal_path)