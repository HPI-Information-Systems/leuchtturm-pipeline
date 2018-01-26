"""This module contains the scheduling of the leuchtturm pipeline."""

from settings import build_name, pipeline_result_path_hdfs_client, file_lister_path_hdfs_client
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from json import dumps
from safygiphy import Giphy


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'schedule_interval': '@once',
    'start_date': datetime(2018, 1, 24),
    'email': ['leuchtturm@example.com'],
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('leuchtturm_pipeline', default_args=default_args)


t1 = BashOperator(
    task_id='clean_and_prepare',
    bash_command="""cd ~
                    """,
    dag=dag
)

t2 = BashOperator(
    task_id='filelister',
    bash_command="""export SPARK_HOME=/usr/hdp/2.6.3.0-235/spark2/
                    export PYSPARK_PYTHON=python3
                    cd ~
                    hdfs dfs -rmr {}
                    spark-submit \
                        --master yarn \
                        --deploy-mode cluster \
                        --driver-memory 4g \
                        --executor-memory 4g \
                        --num-executors 6 \
                        --executor-cores 3 \
                        --py-files ~/pipeline/src/settings.py \
                        ~/pipeline/src/file_lister.py""".format(file_lister_path_hdfs_client),
    dag=dag
)

t2.set_upstream(t1)


t3 = BashOperator(
    task_id='run_leuchtturm_pipeline',
    bash_command="""export SPARK_HOME=/usr/hdp/2.6.3.0-235/spark2/
                    export PYSPARK_PYTHON=python3
                    cd ~
                    hdfs dfs -rmr {}
                    spark-submit \
                        --master yarn \
                        --deploy-mode cluster \
                        --driver-memory 4g \
                        --executor-memory 4g \
                        --num-executors 6 \
                        --executor-cores 3 \
                        --py-files /root/pipeline/src/settings.py,/root/pipeline/src/leuchtturm.py \
                        ~/pipeline/src/run_leuchtturm.py""".format(pipeline_result_path_hdfs_client),
    dag=dag
)

t3.set_upstream(t2)


t4 = BashOperator(
    task_id='write2solr',
    bash_command="""/opt/lucidworks-hdpsearch/solr/bin/solr delete -c {0}
                    /opt/lucidworks-hdpsearch/solr/bin/solr create -c {0} -d leuchtturm_conf -s 2 -rf 2
                    python3 ~/pipeline/src/write_to_solr.py""".format(build_name),
    dag=dag
)

t4.set_upstream(t3)


# t5 = BashOperator(
#     task_id='write2neo4j',
#     bash_command='',
#     dag=dag
# )

# t5.set_upstream(t3)


json_success_message = dumps(
    {"text": "The last pipeline run for succeded. Congrats! :rocket:",
     "attachments": [{"fallback": "View airflow stats at http://b1184.byod.hpi.de:8080.",
                      "color": "#228B22",
                      "text": "You may want to check out this:",
                      "actions": [{"type": "button",
                                   "text": "View Airflow :airplane_departure:",
                                   "url": "http://b1184.byod.hpi.de:8080"},
                                  {"type": "button",
                                   "text": "View Spark history :sparkles:",
                                   "url": "http://b7689.byod.hpi.de:8088/cluster"},
                                  {"type": "button",
                                   "text": "Check Solr :card_file_box:",
                                   "url": "http://b1184.byod.hpi.de:8983"}]},
                     {"text": "This is just an awesome gif reated:",
                      "color": "#228B22",
                      "image_url": "{}"}]}).format(Giphy.random(tag="success"))

notify_success = BashOperator(
    task_id='NotifySuccess',
    bash_command="curl -X POST -H \
                  'Content-type: application/json' --data '{}' \
                  https://hooks.slack.com/services/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"
                 .format(json_success_message),
    trigger_rule='all_success',
    dag=dag
)

notify_success.set_upstream([t1, t2, t3, t4])


json_failure_message = dumps(
    {"text": "Unfortunately, the last pipeline run failed. Keep going! :rotating_light:",
     "attachments": [{"fallback": "View airflow stats at http://b1184.byod.hpi.de:8080.",
                      "color": "#ff0000",
                      "text": "You may want to look into this:",
                      "actions": [{"type": "button",
                                   "text": "View Airflow :wind_blowing_face:",
                                   "url": "http://b1184.byod.hpi.de:8080"},
                                  {"type": "button",
                                   "text": "View Spark history :helmet_with_white_cross:",
                                   "url": "http://b7689.byod.hpi.de:8088/cluster"},
                                  {"type": "button",
                                   "text": "Learn how to access logs :memo:",
                                   "url": "https://hpi.de/naumann/leuchtturm/gitlab/leuchtturm/meta/wikis/home"}]},
                     {"text": "This is just an awesome gif reated:",
                      "color": "#228B22",
                      "image_url": "{}"}]}).format(Giphy.random(tag="fail"))

notify_failure = BashOperator(
    task_id='NotifyFailure',
    bash_command="curl -X POST -H \
                  'Content-type: application/json' --data '{}' \
                  https://hooks.slack.com/services/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"
                 .format(json_failure_message),
    trigger_rule='one_failed',
    dag=dag
)

notify_failure.set_upstream([t1, t2, t3, t4])
