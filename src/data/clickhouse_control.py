"""
author: muzexlxl
email: muzexlxl@foxmail.com

manipulate clickhouse db using both clickhouse-drive and pandahouse
"""
import clickhouse_driver as cd
import pandahouse as ph
import os
import logging
import configparser

import src.data.db_conf as dc

fp = os.path.dirname(__file__)

config = configparser.ConfigParser()
config.read(os.path.join(fp, "../../conf.ini"))

logpath = os.path.join(fp, "../../docs/log.txt")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
filing = logging.FileHandler(filename=logpath)
streaming = logging.StreamHandler()
formatter = logging.Formatter(fmt="%(asctime)s - %(filename)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
filing.setFormatter(formatter)
streaming.setFormatter(formatter)
logger.addHandler(filing)
logger.addHandler(streaming)


class ClickHouse:

    def __init__(self, data_source: str):
        """
        initiate both clickhouse_driver and pandahouse connection via port 9000 / 8123
        Args:
            database: database name
        """
        user = config.get("Clickhouse", "user")
        password = config.get("Clickhouse", "password")
        host = config.get("Clickhouse", "host")

        self.data_source_u = data_source.upper()
        self.data_source_l = data_source.lower()

        self.db_conf = dc.DbConf(self.data_source_l)

        # initiate clickhouse-driver
        self.ck_client = cd.Client(
            user=user, password=password, host=host, port='9000'
        )

        # Check database, See if given database exists, create one if not.
        self.check_ready()

        # initiate pandahouse connection
        self.ph_conn_raw = {
            "host": f"http://{host}:8123",
            "database": self.db_conf.db_raw,
            "user": user,
            "password": password
        }

        self.ph_conn_processed = {
            "host": f"http://{host}:8123",
            "database": self.db_conf.db_processed,
            "user": user,
            "password": password
        }

    def check_ready(self):
        """
        check & create target db and target tables in given database
        """
        for t in self.db_conf.check_ready_sql_generator():
            self.sql_executor(t)

    def sql_executor(self, sql: str):
        """
        execute plain sql through clickhouse-driver
        Args:
            sql:
                show tables => [('TableName',)]
        Returns: raw results fetched from clickhouse db.
        """
        res = self.ck_client.execute(sql)
        return res

    def reader_to_dataframe(
            self,
            sql_query,
            need_handle: bool = True
    ):
        """
        read data from clickhouse
        Args:
            sql_query: sql query used to filter data
            need_handle: if True, all numeric values will be handled to its original value;
                        else it would be the same as it's in clickhouse
        Returns: Dataframe.
        """
        data = ph.read_clickhouse(query=sql_query, index=False, connection=self.ph_conn_raw)
        if need_handle:
            data = data.apply(lambda x: (x/100).astype('float64') if x.dtypes == 'int64' else x, axis=0)
        return data


