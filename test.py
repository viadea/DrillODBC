import pyodbc
import logging
import sys

class StreamToLogger(object):
   """
   Fake file-like stream object that redirects writes to a logger instance.
   """
   def __init__(self, logger, log_level=logging.INFO):
      self.logger = logger
      self.log_level = log_level
      self.linebuf = ''
 
   def write(self, buf):
      for line in buf.rstrip().splitlines():
         self.logger.log(self.log_level, line.rstrip())


# Initialize the logging
logging.basicConfig(
   level=logging.DEBUG,
   format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
   filename="testpy.log",
   filemode='a'
) 

stdout_logger = logging.getLogger('STDOUT')
sl = StreamToLogger(stdout_logger, logging.INFO)
sys.stdout = sl
 
stderr_logger = logging.getLogger('STDERR')
sl = StreamToLogger(stderr_logger, logging.ERROR)
sys.stderr = sl

logging.info('-----------------------------------')
logging.info('Init connection')

conn = pyodbc.connect("DSN=Sample MapR Drill DSN 64", autocommit=True)
cursor = conn.cursor()
 
# Create table SQLs
s1 = "create table dfs.drill.store_sales_1 as select * from dfs.drill.store_sales"
s2 = "create table dfs.drill.store_sales_2 as select * from dfs.drill.store_sales_1"

# Drop table SQLs
d1 = "drop table dfs.drill.store_sales_1"
d2 = "drop table dfs.drill.store_sales_2"

logging.info('Executing S1.')
cursor.execute(s1)
logging.info('Executing S2.')
cursor.execute(s2)
logging.info('Executing D1.')
cursor.execute(d1)
logging.info('Executing D2.')
cursor.execute(d2)

logging.info('Closing cursor.')
cursor.close()
del cursor

logging.info('Closing connection.')
conn.close()
