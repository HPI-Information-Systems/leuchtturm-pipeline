import json
import argparse
import os
import logging
import re
import xml.etree.ElementTree as ET


###########################################################
# FIXME UNFINISHED!
# process_message() and get_body() are not implemented yet
###########################################################
#
# Download Tika App
# > http://www.apache.org/dyn/closer.cgi/tika/tika-app-1.18.jar
#
# Run Tika with '-J' on a pst
# > java -jar tika-app-1.18.jar -J ./data/pst/bill_williams_iii_000_1_1.pst > data/extracted/tmp
#
# Run this script
# > python tika2eml.py data/extracted/tmp data/extraced/eml/


def get_body(message):
    raise NotImplementedError
    return message['X-TIKA:content']


def process_message(message):
    raise NotImplementedError
    # logging.debug('  > ' + str(message.keys()))

    # SAMPLE TIKA OUTPUT:
    #
    # {
    #     "Author": "Williams III\u0000",
    #     "Comments": "",
    #     "Content-Encoding": "windows-1252",
    #     "Content-Type": "text/plain; charset=windows-1252",
    #     "Content-Type-Override": "text/plain",
    #     "Creation-Date": "2009-05-10T22:46:11Z",
    #     "Last-Modified": "2009-05-10T22:46:11Z",
    #     "Last-Save-Date": "2009-05-10T22:46:11Z",
    #     "Message-From": "Williams III\u0000",
    #     "Message:From-Email": "Williams III\u0000",
    #     "Message:From-Name": "Williams III\u0000",
    #     "Message:To-Display-Name": [
    #         "Bland",
    #         "Todd",
    #         "Dean",
    #         "Craig",
    #         "Guzman"
    #     ],
    #     "Message:To-Email": [
    #         "Bland",
    #         "Todd.Bland@ENRON.com",
    #         "Dean",
    #         "Craig.Dean@ENRON.com",
    #         "Guzman"
    #     ],
    #     "X-Parsed-By": [
    #         "org.apache.tika.parser.DefaultParser",
    #         "org.apache.tika.parser.txt.TXTParser"
    #     ],
    #     "X-TIKA:content": "<html xmlns=\"http://www.w3.org/1999/xhtml\">\n
    # <head>\n<meta name=\"date\" content=\"2009-05-10T22:46:11Z\" />\n
    # <meta name=\"Message:To-Email\" content=\"Bland\" />\n
    # <meta name=\"Message:To-Email\" content=\"Todd.Bland@ENRON.com\" />\n
    # <meta name=\"Message:To-Email\" content=\"Dean\" />\n
    # <meta name=\"Message:To-Email\" content=\"Craig.Dean@ENRON.com\" />\n
    # <meta name=\"Message:To-Email\" content=\"Guzman\" />\n
    # <meta name=\"importance\" content=\"1\" />\n
    # <meta name=\"dc:creator\" content=\"Williams III\ufffd\" />\n
    # <meta name=\"dcterms:created\" content=\"2009-05-10T22:46:11Z\" />\n
    # <meta name=\"Message:From-Email\" content=\"Williams III\ufffd\" />\n
    # <meta name=\"dcterms:modified\" content=\"2009-05-10T22:46:11Z\" />\n
    # <meta name=\"Last-Modified\" content=\"2009-05-10T22:46:11Z\" />\n
    # <meta name=\"Last-Save-Date\" content=\"2009-05-10T22:46:11Z\" />\n
    # <meta name=\"meta:mapi-message-class\" content=\"UNKNOWN\" />\n
    # <meta name=\"Message:To-Display-Name\" content=\"Bland\" />\n
    # <meta name=\"Message:To-Display-Name\" content=\"Todd\" />\n
    # <meta name=\"Message:To-Display-Name\" content=\"Dean\" />\n
    # <meta name=\"Message:To-Display-Name\" content=\"Craig\" />\n
    # <meta name=\"Message:To-Display-Name\" content=\"Guzman\" />\n
    # <meta name=\"embeddedRelationshipId\" content=\"&lt;EKDZHLYOL1UDTTI0IQA0BHJYOSW4HBZ5A@zlsvr22&gt;\ufffd\" />\n
    # <meta name=\"flagged\" content=\"false\" />\n<meta name=\"meta:save-date\" content=\"2009-05-10T22:46:11Z\" />\n
    # <meta name=\"dc:title\" content=\"FW: Transmission Losses Presentation\ufffd\" />\n
    # <meta name=\"Content-Encoding\" content=\"windows-1252\" />\n
    # <meta name=\"modified\" content=\"2009-05-10T22:46:11Z\" />\n
    # <meta name=\"displayTo\" content=\"bland; Todd; dean; Craig; guzman\ufffd\" />\n
    # <meta name=\"displayBCC\" content=\"\ufffd\" />\n
    # <meta name=\"Content-Type-Override\" content=\"text/plain\" />\n
    # <meta name=\"Content-Type\" content=\"text/plain; charset=windows-1252\" />\n
    # <meta name=\"identifier\" content=\"&lt;EKDZHLYOL1UDTTI0IQA0BHJYOSW4HBZ5A@zlsvr22&gt;\ufffd\" />\n
    # <meta name=\"creator\" content=\"Williams III\ufffd\" />\n
    # <meta name=\"X-Parsed-By\" content=\"org.apache.tika.parser.DefaultParser\" />\n
    # <meta name=\"X-Parsed-By\" content=\"org.apache.tika.parser.txt.TXTParser\" />\n
    # <meta name=\"displayCC\" content=\"\ufffd\" />\n
    # <meta name=\"meta:author\" content=\"Williams III\ufffd\" />\n
    # <meta name=\"meta:creation-date\" content=\"2009-05-10T22:46:11Z\" />\n
    # <meta name=\"Comments\" content=\"\" />\n
    # <meta name=\"meta:mapi-from-representing-email\" content=\"Williams III\ufffd\" />\n
    # <meta name=\"Creation-Date\" content=\"2009-05-10T22:46:11Z\" />\n
    # <meta name=\"resourceName\" content=\"&lt;EKDZHLYOL1UDTTI0IQA0BHJYOSW4HBZ5A@zlsvr22&gt;\ufffd\" />\n
    # <meta name=\"w:comments\" content=\"\" />\n<meta name=\"priority\" content=\"0\" />\n
    # <meta name=\"senderEmailAddress\" content=\"Williams III\ufffd\" />\n
    # <meta name=\"meta:mapi-from-representing-name\" content=\"Williams III\ufffd\" />\n
    # <meta name=\"recipients\" content=\"No recipients table!\" />\n
    # <meta name=\"Message:From-Name\" content=\"Williams III\ufffd\" />\n
    # <meta name=\"Author\" content=\"Williams III\ufffd\" />\n
    # <meta name=\"comment\" content=\"\" />\n
    # <meta name=\"X-TIKA:embedded_resource_path\" content=\"/&lt;EKDZHLYOL1UDTTI0IQA0BHJYOSW4HBZ5A@zlsvr22&gt;\ufffd\" />\n
    # <meta name=\"Message-From\" content=\"Williams III\ufffd\" />\n
    # <meta name=\"dc:identifier\" content=\"&lt;EKDZHLYOL1UDTTI0IQA0BHJYOSW4HBZ5A@zlsvr22&gt;\ufffd\" />\n
    # <meta name=\"descriptorNodeId\" content=\"2104228\" />\n
    # <title>FW: Transmission Losses Presentation\ufffd</title>\n
    # </head>\n
    # <body><p>Group,\r\n\r\nDave Perrino will be providing us a presentation on losses during the second week of July
    # (tenatively scheduled for the 11th at 4PM).  There is some math/electrical engineering involved. I have attached
    #  the presentation that Dave will give.  Please take about 10-15 minutes to get familiar with the presentation
    # and prepare your questions. \r\n\r\nThanks,\r\nBill\r\n
    # \r\n
    # -----Original Message-----\r\n
    # From: \tPerrino, Dave\r\n
    # Sent:\tTuesday, June 19, 2001 1:34 PM\r\n
    # To:\tWilliams III, Bill\r\nCc:\tWalton, Steve; Alan Comnes/PDX/ECT@ENRON\r\n
    # Subject:\tTransmission Losses Presentation\r\n
    # \r\n
    # Bill,\r\n\r\nAttached to this message is the presentation I am prepared to share with your real-time traders.
    # Please let me know when you'd like to reschedule me to make this presentation.  \r\n\r\n
    # As we discussed a couple weeks ago I have other ideas relating to power systems operations that you may find
    # benefical to share with your trading group.\r\n\r\nKind Regards,\r\n\r\nDave\r\n\r\n \r\n\r\n
    # ***********\r\nEDRM Enron Email Data Set has been produced in EML, PST and NSF format by ZL Technologies, Inc.
    #  This Data Set is licensed under a Creative Commons Attribution 3.0 United States
    # License &lt;http://creativecommons.org/licenses/by/3.0/us/&gt; . To provide attribution, please cite
    # to \"ZL Technologies, Inc. (http://www.zlti.com).\"\r\n***********\r\n\ufffd
    # </p>\n</body></html>",
    #     "X-TIKA:embedded_resource_path": "/<EKDZHLYOL1UDTTI0IQA0BHJYOSW4HBZ5A@zlsvr22>\u0000",
    #     "X-TIKA:parse_time_millis": "0",
    #     "comment": "",
    #     "creator": "Williams III\u0000",
    #     "date": "2009-05-10T22:46:11Z",
    #     "dc:creator": "Williams III\u0000",
    #     "dc:identifier": "<EKDZHLYOL1UDTTI0IQA0BHJYOSW4HBZ5A@zlsvr22>\u0000",
    #     "dc:title": "FW: Transmission Losses Presentation\u0000",
    #     "dcterms:created": "2009-05-10T22:46:11Z",
    #     "dcterms:modified": "2009-05-10T22:46:11Z",
    #     "descriptorNodeId": "2104228",
    #     "displayBCC": "\u0000",
    #     "displayCC": "\u0000",
    #     "displayTo": "bland; Todd; dean; Craig; guzman\u0000",
    #     "embeddedRelationshipId": "<EKDZHLYOL1UDTTI0IQA0BHJYOSW4HBZ5A@zlsvr22>\u0000",
    #     "flagged": "false",
    #     "identifier": "<EKDZHLYOL1UDTTI0IQA0BHJYOSW4HBZ5A@zlsvr22>\u0000",
    #     "importance": "1",
    #     "meta:author": "Williams III\u0000",
    #     "meta:creation-date": "2009-05-10T22:46:11Z",
    #     "meta:mapi-from-representing-email": "Williams III\u0000",
    #     "meta:mapi-from-representing-name": "Williams III\u0000",
    #     "meta:mapi-message-class": "UNKNOWN",
    #     "meta:save-date": "2009-05-10T22:46:11Z",
    #     "modified": "2009-05-10T22:46:11Z",
    #     "priority": "0",
    #     "recipients": "No recipients table!",
    #     "resourceName": "<EKDZHLYOL1UDTTI0IQA0BHJYOSW4HBZ5A@zlsvr22>\u0000",
    #     "senderEmailAddress": "Williams III\u0000",
    #     "title": "FW: Transmission Losses Presentation\u0000",
    #     "w:comments": ""
    # }

    # print(json.dumps(message, indent=2, sort_keys=True))

    msg = ''
    keys = []
    for hp in message.transport_headers.split('\n'):
        pts = re.findall(r'^([^:]+): (.+)\r$', hp)
        if pts:
            key = pts[0][0].capitalize()
            if key in keys:
                key = 'X-' + key
            keys.append(key)
            val = pts[0][1]

            if key == 'Date':
                val = ', '.join(val.split(',')[:2])

            msg += key + ': ' + val + '\r\n'

    msg += 'X-Sender-Name: ' + message.sender_name + '\r\n'
    msg += 'X-Delivery-Time: ' + str(message.delivery_time) + '\r\n'
    msg += 'X-Creation-Time: ' + str(message.creation_time) + '\r\n'
    msg += 'X-Client-Submit-Time: ' + str(message.client_submit_time) + '\r\n'
    msg += 'X-Subject: ' + message.subject + '\r\n'
    msg += 'X-Attachments: ' + str(message.number_of_attachments) + '\r\n'

    btype, body = get_body(message)
    msg += 'X-Body-Type: ' + btype + '\r\n'

    msg += '\r\n'
    msg += body
    return msg


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('PST_FILE', default='data/extracted/tmp',
                        help="Parsed PST file (Tika json output)")
    parser.add_argument('OUTPUT_DIR', default='data/extracted/eml/',
                        help="Directory of output for temporary and report files.")
    parser.add_argument('--logfile', default=None,
                        help='File path of log file.')
    parser.add_argument('--loglevel', default='DEBUG',
                        help='File path of log file.')
    args = parser.parse_args()

    output_directory = os.path.abspath(args.OUTPUT_DIR)

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    if args.logfile:
        if not os.path.exists(args.logfile):
            os.makedirs(args.logfile)
        log_path = os.path.join(args.logfile, 'pst_indexer.log')
    else:
        log_path = None
    logging.basicConfig(level=args.loglevel, filename=log_path,
                        format='%(asctime)s | %(levelname)s | %(message)s', filemode='w')

    logging.info('Starting Script...')

    in_file = os.path.abspath(args.PST_FILE)
    logging.debug('Parsing parsed PST-JSON file: ' + in_file)

    with open(in_file) as fp:
        archive = json.load(fp)
        logging.debug('Read JSON archive.')
        for elem in archive:
            # logging.info(elem.keys())
            logging.debug('Checking element with type: "' + elem['Content-Type'] + '" // filename: ' +
                          elem.get('X-TIKA:embedded_resource_path', 'None ')[:-1])
            if elem['Content-Type'] == 'text/plain; charset=windows-1252':
                eml_file_name = os.path.join(output_directory, elem['identifier'][:-1] + '.eml')
                logging.debug('  > Processing message, will save to: ' + eml_file_name)
                try:
                    with open(eml_file_name, 'w') as eml_file:
                        content = process_message(elem)
                        eml_file.write(content)
                except Exception as e:
                    logging.error('  > FAILED due to: ' + str(e))

                # ['']
                # ['']
                # ['']
                # ['']
