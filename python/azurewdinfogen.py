import os
import sys
import logging
import logzero
import traceback # Python error trace
from logzero import logger

from azure_sql_conn import DatabaseConnection
from diskaccess import DiskAccess

from datetime import datetime

def main(arg=None):
    rst=DatabaseConnection(database='wdinfo').create_truncate_table(tbl='music243'
      , tbl_str="filename text, folder text, type text, fullpath text, filesize bigint, filecreate_dt datetime, inserted_dt datetime default CURRENT_TIMESTAMP")
    logger.info(rst)
    testdir='//192.168.1.243/music'
    dirs, files=DiskAccess().get_dir_file_list_in_folder(thisdir=testdir)
    mystr='\n'.join(dirs)
    logger.info(f"Under {testdir} there exist {len(dirs)} sub folders: {mystr}")
    mystr='\n'.join(files)
    logger.info(f"Under {testdir} there exist {len(files)} files: {mystr}")
    for f in sorted(files):
        status, rst=DiskAccess().file_info(thisfile=f)
        mystr="', '".join(map(str, rst))
        #mystr=f"'{mystr}', '{str(datetime.now())}'"
        mystr=f"'{mystr}'"
        logger.info(mystr)
        query=f"insert into music243 (filename, folder, type, fullpath, filesize, filecreate_dt) values({mystr})"
        rst=DatabaseConnection(database='wdinfo').execute(qry=query)
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