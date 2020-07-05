import os
import sys

import logzero
import traceback # Python error trace
from logzero import logger

import pymssql # Azure SQL server connection

import time

class AzureSqlConnect(object):
    def __init__(self
        , server='sql-sinnud.database.windows.net'
        , user='sinnud@sql-sinnud'
        , password='Jeffery45!@'
        , database=None
        ):
        self.server=server
        self.user=user
        self.password=password
        self.database=database
        self.conn=None

    def connect(self):
        self.conn = pymssql.connect(server=self.server
            , user=self.user
            , password=self.password
            , database=self.database
            )
        self.conn.autocommit(True)
        logger.debug(f"=== Connect to {self.server} using account {self.user} ===")

    def cursor(self):
        if not self.conn: # or self.conn.closed:
            self.connect()
        return self.conn.cursor()

    def execute(self, query):
        if not self.conn: # or self.conn.closed:
            self.connect()
        logger.debug(f'RUNNING QUERY: {query}')
        start = time.time()
        cursor = self.conn.cursor()
        cursor.execute(query)
        logger.debug(f'# ROWS AFFECTED : {cursor.rowcount}, took {time.time() - start} seconds.')
        try:
            rst = cursor.fetchall()
        except Exception as e:
            return cursor
        if len(rst)<1000:
            logger.debug(f'# RESULT : {rst}.')
        return rst        

    ''' load data into MySql on localhost '''
    def local_import(self, file, tbl):
        ''' # do not work. Use Azure storage account instead
        thisquery = f"LOAD DATA LOCAL INFILE '{file}'"
        thisquery = f"{thisquery}\n INTO TABLE {tbl}"
        thisquery = f"{thisquery}\n FIELDS TERMINATED BY ','"
        qt = '"'
        thisquery = f"{thisquery}\n ENCLOSED BY '{qt}'"
        nl = '\\n'
        thisquery = f"{thisquery}\n LINES TERMINATED BY '{nl}'"
        
        thisquery = "BULK"
        thisquery = f"{thisquery}\n INSERT {tbl}"
        thisquery = f"{thisquery}\n FROM {file}"
        thisquery = f"{thisquery}\n WITH (FIELDTERMINATOR = ','"
        thisquery = f"{thisquery}\n       , ROWTERMINATOR = '\n')"
        thisquery = f"{thisquery}\n GO"
        return self.execute(thisquery)
        '''
        pass

    def close(self):
        #if not self.conn.closed:
        logger.debug(f'====== Close {self.server} with account {self.user} ======')
        self.conn.close()

class DatabaseConnection(object):
    def __init__(self, database=None):
        self.database=database
        
    def execute(self, qry=None):
        asc = AzureSqlConnect(database=self.database)
        rst = asc.execute(qry)
        asc.close()
        return rst

    def table_exist(self, tbl=None):
        asc = AzureSqlConnect(database=self.database)
        qry=f"SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
        qry=f"{qry}   AND TABLE_NAME='{tbl}'"
        rst = asc.execute(qry)
        asc.close()
        if rst[0][0]==0:
            return False
        return True

    def truncate_table(self, tbl=None):
        asc = AzureSqlConnect(database=self.database)
        qry=f"truncate table {tbl}"
        rst = asc.execute(qry)
        asc.close()
        return rst

    def create_table(self, tbl=None, tbl_str=None):
        asc = AzureSqlConnect(database=self.database)
        qry=f"create table {tbl} ({tbl_str})"
        rst = asc.execute(qry)
        asc.close()
        return rst
       
    def create_truncate_table(self, tbl=None, tbl_str=None):
        if self.table_exist(tbl=tbl):
            return self.truncate_table(tbl=tbl)
        return self.create_table(tbl=tbl, tbl_str=tbl_str)

def datafile2azureSQLtbl(file=None, asc=None, tbl=None):
    with open(file, "r") as f:
        lines = f.readlines()
    for elm in lines:
        line=elm.replace('\r','').replace('\n','')
        qry=f"insert into {tbl} values ('{line}')"
        rst=asc.execute(qry)
        
    return True

def main(arg=None):
    asc = AzureSqlConnect(database='wdinfo')
    rst = asc.execute('select count(*) from sinnud')
    logger.info(rst)
    #rst = asc.local_import('requirement.txt','sinnud')
    #logger.info(rst)
    rst = datafile2azureSQLtbl(file='requirement.txt', asc=asc, tbl='sinnud')
    asc.close()
    return True
if __name__ == '__main__':
    mylog=os.path.realpath(__file__).replace('.py','.log')
    if os.path.exists(mylog):
        os.remove(mylog)
    logzero.logfile(mylog)

    logger.info(f'start python code {__file__}.\n')
    if len(sys.argv)>1:
        logger.info(f"Argument: {sys.argv}")
        myarg=sys.argv
        pgmname=myarg.pop(0)
        logger.info(f"Program name:{pgmname}.")
        logger.info(f"Arguments:{myarg}.")
        rst=main(arg=' '.join(myarg))
    else:
        logger.info(f"No arguments")
        rst=main()
    if not rst:
        # record error in log such that process.py will capture it
        logger.error(f"ERROREXIT: Please check")
    logger.info(f'end python code {__file__}.\n')