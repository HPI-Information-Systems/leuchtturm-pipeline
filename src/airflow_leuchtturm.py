"""This module contains the scheduling of the leuchtturm pipeline."""

from settings import build_name, pipeline_result_path_hdfs_client, file_lister_path_hdfs_client
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'schedule_interval': '@once',
    'start_date': datetime(2018, 1, 24),
    'email': ['leuchtturm@example.com'],
    'email_on_failure': True,
    'email_on_success': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
}


dag = DAG('leuchtturm_pipeline', default_args=default_args)


t1 = BashOperator(
    task_id='clean_and_prepare',
    bash_command="""cd ~
                    export SPARK_HOME=/usr/hdp/2.6.3.0-235/spark2/
                    export PYSPARK_PYTHON=python3
                    /opt/lucidworks-hdpsearch/solr/bin/solr delete -c {0}
                    /opt/lucidworks-hdpsearch/solr/bin/solr create -c {0} -d data_driven_schema_configs -s 2 -rf 2
                    if [hdfs dfs -test -e {1}]; then hdfs dfs -rmr {1} fi
                    if [hdfs dfs -test -e {2}]; then hdfs dfs -rmr {2} fi
                    """.format(build_name, pipeline_result_path_hdfs_client, file_lister_path_hdfs_client),
    dag=dag
)

t2 = BashOperator(
    task_id='filelister',
    bash_command='spark-submit \
                  --master yarn \
                  --deploy-mode cluster \
                  --driver-memory 4g \
                  --executor-memory 4g \
                  --num-executors 6 \
                  --executor-cores 3 \
                  --py-files ~/pipeline_new/src/settings.py \
                  ~/pipeline_new/src/file_lister.py',
    dag=dag
)


t3 = BashOperator(
    task_id='run_leuchtturm_pipeline',
    bash_command='spark-submit \
                  --master yarn \
                  --deploy-mode cluster \
                  --driver-memory 4g \
                  --executor-memory 4g \
                  --num-executors 6 \
                  --executor-cores 3 \
                  --py-files ~/pipeline_new/src/settings.py,~/pipeline_new/src/leuchtturm.py \
                  ~/pipeline_new/src/run_leuchtturm.py',
    dag=dag
)

t3.set_upstream(t2)


t4 = BashOperator(
    task_id='write2solr',
    bash_command='python3 ~/pipeline_new/src/write_to_solr.py',
    dag=dag
)

t4.set_upstream(t3)


# t5 = BashOperator(
#     task_id='write2neo4j',
#     bash_command='date',
#     dag=dag
# )

# t5.set_upstream(t3)


json_success_message = '{"text":"Success :thumbs_up:"}'

notify_success = BashOperator(
    task_id='NotifySuccess',
    bash_command="curl -X POST -H \
                  'Content-type: application/json' --data '{}' \
                  https://hooks.slack.com/services/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"
                 .format(json_success_message),
    trigger_rule='all_success',
    dag=dag
)


json_failure_message = '{"text":"Fail :("}'

notify_success = BashOperator(
    task_id='NotifyFailure',
    bash_command="curl -X POST -H \
                  'Content-type: application/json' --data '{}' \
                  https://hooks.slack.com/services/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"
                 .format(json_failure_message),
    trigger_rule='all_failed',
    dag=dag
)
