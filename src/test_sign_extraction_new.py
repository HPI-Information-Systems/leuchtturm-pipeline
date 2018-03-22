"""Test the signature extraction script against an annotated dataset."""
from leuchtturm import extract_signature_information
import os
import json
import re

# directory that only contains the '*.txt.ann' files
annotated_mails_path = "/Users/j/Uni/BP/code/pipeline/raw_quagga_annotated_detailled_train_curated_ann"
results_path = "/Users/j/Uni/BP/code/pipeline/results_quagga/part-00000"

existent_signatures_recognized = 0
existent_signatures_recognized_correctly = 0
total_mails_with_quagga_signatures = 0

with open(results_path) as results:
    results = results.read()
    results = re.sub('\n{', ', \n{', results)
    results = '[' + results + ']'
    results = json.loads(results)

    for result in results:
        with open(annotated_mails_path + '/' + result['doc_id'] + '.txt.ann') as annotated_res:
            annotated_res = json.loads(annotated_res.read())
            signature_denotation_text = \
                [den['text'] for den in annotated_res['denotations'] if den['type'] == "Body/Signature"]
            if signature_denotation_text:
                total_mails_with_quagga_signatures += 1
                if result['signature']:
                    existent_signatures_recognized += 1
                    if signature_denotation_text[0].strip() == result['signature'].strip():
                        existent_signatures_recognized_correctly += 1
                else:
                    print('\n\n\n\n\n')
                    print('unrecognized')
                    print(annotated_mails_path + '/' + result['doc_id'] + '.txt')
                    print('----------------quagga signature(s)--------------------------------------------------------')
                    print(signature_denotation_text)
                    print('----------------body-----------------------------------------------------------------------')
                    print(result['body'])
                    print('----------------body without signature-----------------------------------------------------')
                    print(result['body_without_signature'])

print('total_mails_with_quagga_signatures', total_mails_with_quagga_signatures)
print('signatures_recognized', existent_signatures_recognized)
print('signatures_recognized_correctly', existent_signatures_recognized_correctly)
