from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_sql = """
                    INSERT INTO {}
                    {}
                    ;
                 """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                sql_source="",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_source = sql_source

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql_source
        )
        self.log.info(f"Executing {formatted_sql} ...")
        redshift.run(formatted_sql)



# from airflow.hooks.postgres_hook import PostgresHook
# from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults

# class LoadFactOperator(BaseOperator):
#     ui_color = '#F98866'
    
#     @apply_defaults
        
#     def __init__(self,
#         redshift_conn_id="",
#         table="",
#         table_columns="",
#         sql_to_load_tbl ="",
#         *args, **kwargs):

#         super(LoadFactOperator, self).__init__(*args, **kwargs)
#             self.redshift_conn_id = redshift_conn_id
#             self.table = table
#             self.table_columns = table_columns
#             self.sql_to_load_tbl = sql_to_load_tbl

#     def execute(self, context):
#         self.log.info('LoadFactOperator Running')
#         redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
#         sql_statement = "INSERT INTO {} {} {}".format(self.table, self.table_columns,self.sql_to_load_tbl)
#         redshift.run(sql_statement)




# class LoadFactOperator(BaseOperator):
#     ui_color = '#F98866'
#     template_fields = ("s3_key",)
#     ui_color = '#358140'
#     copy_sql = """SELECT md5(events.sessionid || events.start_time) songplay_id,
#                          events.start_time, 
#                          events.userid, 
#                          events.level, 
#                          songs.song_id, 
#                          songs.artist_id, 
#                          events.sessionid, 
#                          events.location, 
#                          events.useragent
#                          FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
#                   FROM staging_events
#                   WHERE page='NextSong') events
#                   LEFT JOIN staging_songs songs
#                   ON events.song = songs.title
#                      AND events.artist = songs.artist_name
#                      AND events.length = songs.duration
#                   ACCESS_KEY_ID '{}'
#                   SECRET_ACCESS_KEY '{}'
#                """
    
#     @apply_defaults
#     def __init__(self,
#                  redshift_conn_id="",
#                  aws_credentials_id="",
#                  table="",
#                  s3_bucket="",
#                  s3_key="",
#                  *args, **kwargs):

#         super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
#         self.table = table
#         self.redshift_conn_id = redshift_conn_id
#         self.s3_bucket = s3_bucket
#         self.s3_key = s3_key
#         self.aws_credentials_id = aws_credentials_id

#     def execute(self, context):
#         self.log.info('Loading fact table')
#         aws_hook = AwsHook(self.aws_credentials_id)
#         credentials = aws_hook.get_credentials()
#         redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
# #         redshift.run("DELETE FROM {}".format(self.table))
#         rendered_key = self.s3_key.format(**context)
# #         s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
#         formatted_sql = StageToRedshiftOperator.copy_sql.format(
#             credentials.access_key,
#             credentials.secret_key
#         )
#         redshift.run(formatted_sql)
