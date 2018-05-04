import sys

for i in range(2, 25):
    print('looking at log', i)
    non_cooccurrences = set()
    start_sig_extraction_count = 0
    finish_sig_extraction_count = 0

    with open(sys.argv[1] + '/' + str(i) + '.txt') as f:
        for line in f:
            if 'START SIG: ' in line:
                print('got to correspondent data extraction')
            elif 'Starting to run signature extraction on partition..' in line:
                start_sig_extraction_count += 1
            elif 'Finished running signature extraction on partition.' in line:
                finish_sig_extraction_count += 1
            elif ('run_on_partition: S 2018' in line) or ('run_on_partition: E 2018' in line):
                timestamp = line.rsplit('run_on_partition: ', 1)[1][2:]
                if timestamp in non_cooccurrences:
                    non_cooccurrences.remove(timestamp)
                else:
                    non_cooccurrences.add(timestamp)

    print('start sig extraction count', start_sig_extraction_count)
    print('finish sig extraction count', finish_sig_extraction_count)
    print(non_cooccurrences)
    print('\n')
