"""Test the signature extraction script against an annotated dataset."""
from leuchtturm import extract_signature_information
import os
import json

# directory that only contains the '*.txt.ann' files
annotated_mails_path = "/Users/j/Uni/BP/code/pipeline/annotated_quagga_train_mails"
# directory that only contains the '*.txt' files
raw_mails_path = "/Users/j/Uni/BP/code/pipeline/raw_quagga_train_mails"

existent_signatures_recognized = 0
existent_signatures_recognized_correctly = 0
total_mails_with_quagga_signatures = 0

for filename in os.listdir(raw_mails_path):
    print_stuff = ''
    if filename.endswith('.txt'):
        with open(raw_mails_path + '/' + filename) as mail:
            mail = mail.read()
            try:
                f = open(annotated_mails_path + '/' + filename + '.ann')
            except IOError:
                print('no annotated mail found')
                continue
            with f as annotated_mail:
                annotated_mail = json.loads(annotated_mail.read())
                signature_denotation_text = \
                    [den['text'] for den in annotated_mail['denotations'] if den['type'] == "Body/Signature"]
                if signature_denotation_text:
                    total_mails_with_quagga_signatures += 1
                    temp = {
                        'body': mail,
                        'header': {
                            'sender': {
                                'email': annotated_mail['meta']['header']['From']
                            }
                        }
                    }
                    mail_w_signature = json.loads(extract_signature_information(json.dumps(temp), test_mode=True))
                    if signature_denotation_text and mail_w_signature['signature']:
                        existent_signatures_recognized += 1
                    else:
                        print_stuff = 'unrecognized '
                    if signature_denotation_text \
                            and mail_w_signature['signature'] \
                            and signature_denotation_text[0].strip() == mail_w_signature['signature'].strip():
                        existent_signatures_recognized_correctly += 1
                    else:
                        print_stuff += '+ incorrectly '
                    print('\n\n\n')
                    print(print_stuff)
                    print('----quagga signature-----')
                    print(signature_denotation_text[0])
                    print('----my extracted signature-----')
                    print(mail_w_signature['signature'])
                    print('----original email------')
                    print(annotated_mails_path + '/' + filename + '.ann')
                    print(mail)

print('total_mails_with_quagga_signatures', total_mails_with_quagga_signatures)
print('signatures_recognized', existent_signatures_recognized)
print('signatures_recognized_correctly', existent_signatures_recognized_correctly)
