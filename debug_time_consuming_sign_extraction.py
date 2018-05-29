# flake8: noqa
import sys
import os
from datetime import datetime

START_STRING = ' WARNING signature_extraction - run_on_partition: S '
END_STRING = ' WARNING signature_extraction - run_on_partition: E '
SPLIT_PATH_PREFIX = 'hdfs://odin01.hpi.uni-potsdam.de:8020/user/leuchtturm/'
SPLIT_BODY_LENGTH_PREFIX = ' WARNING signature_extraction - run_on_partition: LENGTH: '
MAX_DELAY_SECONDS = 1
TIME_FORMAT = '%H:%M:%S,%f'

start_count = 0
end_count = 0

for folder, subdirs, files in os.walk(sys.argv[1]):
    for filename in files:
        if filename == '.DS_Store':
            continue
        with open(os.path.join(folder, filename), 'r') as f:
            save_next = -1
            start_time = 0
            end_time = 0
            path = ''
            body_length = 0
            body_start = ''
            body_end = ''
            for line in f:
                if save_next == 4 and SPLIT_PATH_PREFIX in line:
                    path = line.split(SPLIT_PATH_PREFIX)[1]
                    save_next -= 1
                elif save_next == 3 and SPLIT_BODY_LENGTH_PREFIX in line:
                    body_length = int(line.split(SPLIT_BODY_LENGTH_PREFIX)[1])
                    save_next -= 1
                elif save_next == 2 and 'START: ' in line:
                    body_start = line.split('START: ')[1]
                    save_next -= 1
                elif save_next == 1 and 'END: ' in line:
                    body_end = line.split('END: ')[1]
                    save_next -= 1
                elif save_next == 0 and END_STRING in line:
                    end_time_str = line.split(END_STRING)[0].split('.')[0].split(' ')[1]
                    end_time = datetime.strptime(end_time_str, '%H:%M:%S,%f')
                    if (end_time - start_time).total_seconds() > MAX_DELAY_SECONDS:
                        print(start_time)
                        print(end_time)
                        print(path)
                        print(body_length)
                        print(body_start)
                        print(body_end)
                    start_time = 0
                    end_time = 0
                    path = ''
                    body_length = 0
                    body_start = ''
                    body_end = ''
                    save_next -= 1
                elif 'Starting to run signature extraction on partition...' in line:
                    start_count += 1
                elif 'Finished running signature extraction on partition.' in line:
                    end_count += 1
                elif START_STRING in line:
                    start_time_str = line.split(START_STRING)[0].split('.')[0].split(' ')[1]
                    start_time = datetime.strptime(start_time_str, '%H:%M:%S,%f')
                    save_next = 4

print('# sign extraction starts', start_count)
print('# sign extraction ends', end_count)

