import datetime as dt
from sqlalchemy import create_engine, Column, DateTime, Float, String, text, inspect, BigInteger, UniqueConstraint, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
import yfinance as yf


class stock_prices:
    def __init__(self):
        self.engine = create_engine('sqlite:///StockPrices.db')
        return


    def create_tbl(self, tablename):
        # Initialize the declarative base
        Base = declarative_base()

        class table_meta_data(Base):
            __tablename__ = tablename
            Date = Column(DateTime, primary_key=True)
            Stock = Column(String(50), primary_key=True)
            Time_Interval = Column(String(20), primary_key=True, default='1M')
            Open = Column(Float)
            Close = Column(Float)
            High = Column(Float)
            Low = Column(Float)
            Adj_Close = Column(Float)
            Volume = Column(BigInteger)
            __table_args__ = (PrimaryKeyConstraint('Date', 'Stock', 'Time_Interval', name='uniq_PK1', sqlite_on_conflict='IGNORE'),)

        insp = inspect(self.engine)
        # Create the table and all the fields needed as well as the composite primary key
        if not insp.has_table(tablename):
            try:
                print(f'Create {tablename} table...')
                Base.metadata.create_all(self.engine)
            except Exception as e:
                print(f'(create_{tablename}_tbl) Something went wront during the creation of the {tablename} table via SQLAlchemy. Error: {str(e)}')
        else:
            # since the table exists check that all the columns and primary key exists
            primary_keys_added = False
            table_columns = insp.get_columns(tablename)
            sql_statements =[]
            if not any(['Date' in i.values() for i in table_columns]):
                sql = f'ALTER TABLE {tablename} ADD COLUMN Date DATETIME NOT NULL'
                sql_statements.append(sql)
                primary_keys_added = True
            if not any(['Stock' in i.values() for i in table_columns]):
                sql = f'ALTER TABLE {tablename} ADD COLUMN Stock VARCHAR(50) NOT NULL'
                sql_statements.append(sql)
                primary_keys_added = True
            if not any(['Time_Interval' in i.values() for i in table_columns]):
                sql = f'ALTER TABLE {tablename} ADD COLUMN Time_Interval VARCHAR(20) NOT NULL DEFAULT "1M"'
                sql_statements.append(sql)
                primary_keys_added = True
            if primary_keys_added:
                # https://stackoverflow.com/questions/1884787/how-do-i-drop-a-constraint-from-a-sqlite-3-6-21-table
                # This drop primarykey, will not work in SQLite, please see the article above
                '''
                pk = insp.get_pk_constraint(tablename)
                if len(pk['constrained_columns']) > 0:
                    sql = f'ALTER TABLE {tablename} DROP PRIMARY KEY'
                    sql_statements.append(sql)
                '''
                sql = f'ALTER TABLE {tablename} ADD PRIMARY KEY(Date, Stock, Time_Interval) ON CONFLICT IGNORE'
                sql_statements.append(sql)
            if not any(['Open' in i.values() for i in table_columns]):
                sql = f'ALTER TABLE {tablename} ADD COLUMN Open FLOAT'
                sql_statements.append(sql)
            if not any(['Close' in i.values() for i in table_columns]):
                sql = f'ALTER TABLE {tablename} ADD COLUMN Close FLOAT'
                sql_statements.append(sql)
            if not any(['High' in i.values() for i in table_columns]):
                sql = f'ALTER TABLE {tablename} ADD COLUMN High FLOAT'
                sql_statements.append(sql)
            if not any(['Low' in i.values() for i in table_columns]):
                sql = f'ALTER TABLE {tablename} ADD COLUMN Low FLOAT'
                sql_statements.append(sql)
            if not any(['AdjClose' in i.values() for i in table_columns]):
                sql = f'ALTER TABLE {tablename} ADD COLUMN AdjClose FLOAT'
                sql_statements.append(sql)
            if not any(['Volume' in i.values() for i in table_columns]):
                sql = f'ALTER TABLE {tablename} ADD COLUMN Volume BIGINT'
                sql_statements.append(sql)

            for sql_statement in sql_statements:
                print(sql_statement)
                try:
                    self.engine.execute(sql_statement)
                except Exception as e:
                    print(f'(create_analysis_tbl) Something went wront during the update of column(s) or primary key in analysis table via SQLAlchemy. Error: {str(e)}')
                    print(sql_statement)
        return True


    def get_prices(self, stock, start_date, end_date):
        # fetch gold prices using Yahoo Finance API
        ticker = stock
        if stock.upper() == 'SP500' or stock.upper() == '^GSPC':
            ticker = '^GSPC'
        if stock.upper() == 'GOLD' or stock.upper() == 'GC=F':
            ticker = 'GC=F'
        df = yf.download(ticker, start=start_date, end=end_date, interval='1mo', progress=False)
        if df is None or df.empty or len(df) < 1:
            return None
        column_to_add = 'Stock'
        column_list = [column_to_add]+list(df.columns)+['Time_interval']
        df[column_to_add] = stock.upper()
        df['Time_interval'] = '1M'
        df = df[column_list]
        df = df.rename(columns={'Adj Close':'Adj_Close'})
        return df


    def get_growth(self, ticker, years, store_data=True):
        end_date = dt.datetime.now()
        start_date = end_date - dt.timedelta(days=years * 365)

        df = self.get_prices(ticker, start_date, end_date)

        start_price = df.head(1).Adj_Close.iloc[0]
        end_price = df.tail(1).Adj_Close.iloc[0]
        start_ticket_date = df.head(1).index[0]
        end_ticket_date = df.tail(1).index[0]
        growth_p = (end_price-start_price)/start_price*100
        ticker_years = (end_ticket_date-start_ticket_date).days/365.25
        print(f'{ticker} growth over the last {ticker_years:,.1f} years, is {growth_p/ticker_years:,.2f}% per year')

        if store_data:
            tablename = 'StockPrice'
            self.create_tbl(tablename=tablename)
            df.to_sql(tablename, self.engine, if_exists='append', index=True, chunksize=100)


if __name__ == '__main__':
    sp = stock_prices()

    # set start and end date for the time period we want to fetch data for
    years = 5
    sp.get_growth('SP500', years)
    sp.get_growth('Gold', years)
    sp.get_growth('FNZ.NZ', years)
    sp.get_growth('BOT.NZ', years)
    sp.get_growth('LIV.NZ', years)
