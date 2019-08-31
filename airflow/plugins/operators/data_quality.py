'''
Data quality
'''
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality = data_quality

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        param = self.data_quality

        for n in range(len(param)):
            if(param[n]['type'] == 'count'):
                query = "SELECT COUNT(*) FROM {}"
                query = query.format(param[n]['table'])
                record = redshift.get_records(query)
                if(record[0][0] > param[n]['result']):
                    self.log.info(f"Param {n} pass in data quality check")
                else:
                    raise ValueError(f"Data quality check failed in param {n}")
                    self.log.info(f"Quality checks fail in param: {n}")
            elif(param[n]['type'] == 'count-where'):
                query = "SELECT COUNT(*) FROM {} WHERE {} IS NULL"
                query = query.format(param[n]['table'],param[n]['field'])
                record = redshift.get_records(query)
                if(record[0][0] == param[n]['result']):
                    self.log.info(f"Param {n} pass in data quality check")
                else:
                    raise ValueError(f"Data quality check failed in param {n}")
                    self.log.info(f"Quality checks fail in param: {n}")
            else:
                self.log.info("Wrong type param : {}".format(param[n]['type'] ))
        
        self.log.info("QUality checks ok")
       
       