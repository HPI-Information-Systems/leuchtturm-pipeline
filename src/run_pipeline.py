import os
import numpy as np
from hdfs import InsecureClient


source_dir = "/user/admin/enron/TEXT"
client = InsecureClient('http://b7689.byod.hpi.de:50070')

for dir in client.list(source_dir):
                for subdir in client.list(source_dir + '/' + dir):
                    os.system('PYTHONPATH="." luigi --module spark_pipeline WriteToSolr --FileLister-path ' + '"' + source_dir + '/' + dir + '/' + subdir + '"')