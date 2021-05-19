class DbConf:
    def __init__(self, data_source: str):
        self.data_source = data_source

        self.db_raw = 'raw'
        self.db_processed = 'processed'

        self.raw_trade_data = f"trade_data_{self.data_source}"
        self.raw_contract_data = f'contract_data_{self.data_source}'

        self.processed_trade_data_main = f'trade_data_main_{self.data_source}'

        self.db_structure = {
            self.db_raw: [
                self.raw_contract_data,
                self.raw_trade_data,
            ],
            self.db_processed: [
                self.processed_trade_data_main,
            ]
        }

    def check_ready_sql_generator(self):
        for k, v in self.db_structure.items():
            db = k
            yield f"CREATE DATABASE IF NOT EXISTS {db} engine=Atomic"
            for table in v:
                if table.split('_')[0] == 'trade':
                    yield self.create_trade_data_table(db, table)
                elif table.split('_')[0] == 'contract':
                    yield self.create_contract_data_table(db, table)

    @staticmethod
    def create_trade_data_table(database: str, datatable: str):
        return f"CREATE TABLE IF NOT EXISTS {database}.{datatable}" \
               "(`code` String NOT NULL," \
               "`symbol` String NOT NULL," \
               "`datetime` Datetime NOT NULL," \
               "`open` Int64," \
               "`close` Int64," \
               "`high` Int64," \
               "`low` Int64," \
               "`limit_up` Int64," \
               "`limit_down` Int64," \
               "`volume` Int64," \
               "`turnover` Int64," \
               "`change` Int64," \
               "`change_pctg` Int64," \
               "`pos_change` Int64," \
               "`amplitude` Int64," \
               "`open_interest` Int64," \
               "`timeframe` String," \
               "`data_source` String," \
               "`data_type` String)" \
               "engine=MergeTree()" \
               "ORDER BY (`code`, `datetime`, `symbol`, `timeframe`)" \
               "PARTITION BY `symbol`"

    @staticmethod
    def create_contract_data_table(database: str, datatable: str):
        return f"CREATE TABLE IF NOT EXISTS {database}.{datatable}" \
               "(`code` String NOT NULL," \
               "`security_type` String," \
               "`underlying` String," \
               "`ths_code` String NOT NULL," \
               "`trade_unit` Int64 NOT NULL," \
               "`multiplier` Int64," \
               "`price_unit` String," \
               "`tick_price` Int64," \
               "`change_limit` Int64," \
               "`target_margin` Int64," \
               "`speculate_long_margin` Int64," \
               "`speculate_short_margin` Int64," \
               "`hedging_long_margin` Int64," \
               "`hedging_short_margin` Int64," \
               "`transaction_rate` Int64," \
               "`transaction_fee` Int64," \
               "`t0_close_transaction_rate` Int64," \
               "`t0_close_transaction_fee` Int64," \
               "`listed_date` String NOT NULL," \
               "`maturity_date` String," \
               "`benchmark_price` Int64," \
               "`initial_margin` Int64," \
               "`last_delivery_date` String," \
               "`symbol` String," \
               "`exchange` String," \
               "`exchange_name` String," \
               "`data_source` String)" \
               "engine=ReplacingMergeTree()" \
               "ORDER BY `code`" \
               "PARTITION BY `symbol`"

