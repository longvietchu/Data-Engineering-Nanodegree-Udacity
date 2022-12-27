import pandas as pd

from sqlalchemy import create_engine, text

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class SASValueToRedshiftOperator(BaseOperator):
    """
        Custom Operator for extracting data from SAS source code.
    """
    ui_color = '#358150'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 sas_value="",
                 columns="",
                 *args, **kwargs):
        """
        Extracts label mappings from SAS source code and store as Redshift table
            Args:
                aws_credentials_id (string): Airflow connection ID for AWS key and secret.
                redshift_conn_id (string): Airflow connection ID for Redshift.
                table (string): Name of table to be loaded.
                s3_bucket (string): S3 Bucket Name Where SAS source code is store.
                s3_key (string): S3 Key Name for SAS source code.
                sas_value (string): Value to search for in SAS file for data extraction.
                columns (list): Column names of the result data.
        """
        super(SASValueToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sas_value = sas_value
        self.columns = columns
    
    def execute(self, context):
        s3 = S3Hook(self.aws_credentials_id)

        redshift_conn = BaseHook.get_connection(self.redshift_conn_id)
        self.log.info('Connecting to {}...'.format(redshift_conn.host))
        conn = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
                             redshift_conn.login,
                             redshift_conn.password,
                             redshift_conn.host,
                             redshift_conn.port,
                             redshift_conn.schema
                            ))
        self.log.info('Connected!')

        self.log.info('Reading From S3: s3://{}/{}'.format(self.s3_bucket, self.s3_key))
        file_string = s3.read_key(self.s3_key, self.s3_bucket)
        self.log.info('File has {} characters'.format(len(file_string)))

        file_string = file_string[file_string.index(self.sas_value):]
        file_string = file_string[:file_string.index(';')]
        
        line_list = file_string.split('\n')[1:]
        codes = []
        values = []
        
        self.log.info('Parsing SAS file: {}/{}'.format(self.s3_bucket, self.s3_key))
        for line in line_list:
            
            if '=' in line:
                code, val = line.split('=')
                code = code.strip()
                val = val.strip()
                
                if code[0] == "'":
                    code = code[1:-1]
                    
                if val[0] == "'":
                    val = val[1:-1]
                    
                codes.append(code)
                values.append(val)

        self.log.info('Converting parsed data to dataframe...')
        df = pd.DataFrame(list(zip(codes,values)), columns=self.columns)

        self.log.info(f'Truncating table: {self.table}')
        truncate_query = text(f'TRUNCATE TABLE {self.table}')
        conn.execution_options(autocommit=True).execute(truncate_query)

        self.log.info('Writing result to table {}'.format(self.table))
        df.to_sql(self.table, conn, index=False, if_exists='append')
        conn.dispose()
