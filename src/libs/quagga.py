"""Quagga is an email segmentation system written by Tim Repke.

https://github.com/TimRepke/Quagga
Ref. Repke, Tim and Krestel R. Bringing Back Structure to Free Text Email Conversations with Recurrent Neural Networks. ECIR 2018.
"""

import keras.backend as K
from keras_contrib.utils import save_load_utils
from keras.models import model_from_json, Model
from keras.layers import Masking, GRU, Input, Bidirectional
from keras_contrib.layers import CRF
import numpy as np
from sklearn.preprocessing import LabelEncoder
import re


line_embedding_size = 32


def _load_keras_model(path, model=None):
    with open(path + '.json', 'r') as jf:
        json_model = jf.read()
    if model is None:
        model = model_from_json(json_model)
    try:
        save_load_utils.load_all_weights(model, path + '.hdf5')
    except KeyError:
        model.load_weights(path + '.hdf5')

    return model


def _get_mail_model_two():
    output_size = 2
    in_mail = Input(shape=(None, line_embedding_size), dtype='float32')

    mask = Masking()(in_mail)
    hidden = Bidirectional(GRU(32 // 2,
                               return_sequences=True,
                               implementation=0))(mask)
    crf = CRF(output_size, sparse_target=False)  # , test_mode='marginal', learn_mode='marginal')
    output = crf(hidden)

    model = Model(inputs=in_mail, outputs=output)
    # model.compile(loss=crf.loss_function, optimizer='adam', metrics=[crf.accuracy])
    return model


def _get_embedding_function(model):
    model_in = [model.input]
    embedding_func = K.function(model_in + [K.learning_phase()], [model.layers[-2].output])

    def lambdo(x):
        return embedding_func([x, 0.])[0]

    return lambdo


enron_two_zone_line_b = _load_keras_model('.//models/enron_line_model_b')
enron_two_zone_model = _load_keras_model('./models/enron_model', model=_get_mail_model_two())
# asf_two_zone_line_b = load_keras_model('./emailbody/models/two_zones/asf_line_model_b')
# asf_two_zone_model = load_keras_model('./emailbody/models/two_zones/asf_model', model=get_mail_model_two())
enron_two_zone_line_b_func = _get_embedding_function(enron_two_zone_line_b)
# asf_two_zone_line_b_func = get_embedding_function(asf_two_zone_line_b)

two_encoder = LabelEncoder().fit(['Body', 'Header'])

char_index = list(' '
                  'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                  'abcdefghijklmnopqrstuvwxyz'
                  '0123456789'
                  '@€-_.:,;#\'+*~\?}=])[({/&%$§"!^°|><´`\n')
num_possible_chars = len(char_index)
line_length = 80


def _embed(lines, embedding_functions=None):
    x = np.zeros((len(lines), line_length, num_possible_chars + 1))

    for i, line in enumerate(lines):
        for j, c in enumerate(line):
            if j >= line_length:
                break
            x[i][j][char_index.index(c) + 1 if c in char_index else 0] = 1

    if embedding_functions is None:
        return x

    x = np.concatenate([embedding_function(x) for embedding_function in embedding_functions], axis=1)

    return x


def prediction2response(y, text_lines, label_encoder):
    labels = label_encoder.classes_
    ret = []
    for yi, line in zip(y, text_lines):
        tmp = {
            'text': line,
            'predictions': {}
        }
        for li, label in enumerate(labels):
            tmp['predictions'][label] = yi[li]
        ret.append(tmp)

    return ret  # jsonify(ret)


# def five(text_raw, trained_on='enron', use_model='crf'):
#     text_lines = text_raw.split('\n')

#     if trained_on == 'enron':
#         func_a = enron_five_zone_line_a_func
#         func_b = enron_five_zone_line_b_func
#         model = enron_five_zone_model
#         embedding_a = enron_five_zone_line_a
#     else:
#         func_a = asf_five_zone_line_a_func
#         func_b = asf_five_zone_line_b_func
#         model = asf_five_zone_model
#         embedding_a = asf_five_zone_line_a

#     if use_model == 'crf':
#         text_embedded = _embed(text_lines, [func_a, func_b])
#         y = model.predict(np.array([text_embedded])).tolist()[0]
#         return prediction2response(y, text_lines, five_encoder)
#     return prediction2response(embedding_a.predict(_embed(text_lines)).tolist(), text_lines, five_encoder)


def two(text_raw, trained_on='enron', use_model='crf'):
    text_lines = text_raw.split('\n')

    if trained_on == 'enron':
        func_b = enron_two_zone_line_b_func
        model = enron_two_zone_model
        embedding_b = enron_two_zone_line_b
    # else:
    #     func_b = asf_two_zone_line_b_func
    #     model = asf_two_zone_model
    #     embedding_b = asf_two_zone_line_b

    if use_model == 'crf':
        text_embedded = _embed(text_lines, [func_b])
        y = model.predict(np.array([text_embedded])).tolist()[0]
        return prediction2response(y, text_lines, two_encoder)
    return prediction2response(embedding_b.predict(_embed(text_lines)).tolist(), text_lines, two_encoder)


def detect_parts(si):
    def tp(preds):
        return sorted(preds.items(), key=lambda x: x[1], reverse=True)[0][0]

    pred = two(si)  # , trained_on='asf')

    # blockss = []
    blocks = []
    # pre-filled info comes from email protocol header
    curr_block = {
        'from': '',
        'to': '',
        'cc': '',
        'sent': '',
        'subject': '',
        'type': 'root',  # root, forward, reply
        'raw_header': [],
        'text': []
    }
    # modes: 0 = eat body, 1 = eat forward, 2 = eat reply,
    #        3 = eat from, 4 = eat to, 5 = eat cc/bcc, 6 = sent, 7 = eat subject
    mode = 0
    for line in pred:
        line_prediction = tp(line['predictions'])
        line_low = line['text'].lower()
        if line_prediction == 'Header':
            # looks like the root block ends here...
            if mode == 0:
                blocks.append(curr_block)
                next_mode = 1 if 'forward' in line_low else 2
                curr_block = {'from': None, 'to': None, 'cc': None, 'sent': None, 'subject': None,
                              'type': 'forward' if next_mode == 1 else 'reply',
                              'raw_header': [],
                              'text': []}
                mode = next_mode

            # stop eating forward header when seeing "-----Original Message-----"
            if mode == 1 and 'original' in line_low:
                blocks.append(curr_block)
                curr_block = {'from': None, 'to': None, 'sent': None, 'subject': None,
                              'type': 'reply', 'raw_header': [line['text']], 'text': []}
                mode = 2
                # nothing else to expect from this line, carry on!
                # note: there are cases where newlines are missing, ...
                continue

            # forward header in one line
            # ---------------------- Forwarded by Sherri Sera/Corp/Enron on 04/20/2001 12:21 PM --------------------
            if mode == 1 and re.match(r"-+ ?forward.+?-+", line_low):
                curr_block['raw_header'].append(line['text'])

                grps = re.search(r"-+ Forward(?:ed)? by (.+?) on (.+?)-+", line['text'], flags=re.IGNORECASE)
                try:
                    curr_block['from'] = grps.group(1)
                    curr_block['sent'] = grps.group(2)
                except Exception:
                    curr_block['from'] = ''
                    curr_block['sent'] = ''

                # take info from previous block
                curr_block['to'] = blocks[-1]['to']
                curr_block['subject'] = blocks[-1]['subject']

                curr_block = {'from': None, 'to': None, 'cc': None, 'sent': None, 'subject': None,
                              'type': None, 'raw_header': [], 'text': []}

                # next up: zombie mode (eat bodies)
                mode = 0

                # nothing else to expect from this line, carry on as zombie!
                continue

            # forward header in two lines
            # ---------------------- Forwarded by Charlotte Hawkins/HOU/ECT on 04/04/2000
            # 01:37 PM ---------------------------
            # TODO: are there more messed up cases?
            if mode == 1:
                curr_block['raw_header'].append(line['text'])

                try:
                    # try eating the first line
                    grps = re.search(r"-+ Forward(?:ed)? by (.+)", line['text'], flags=re.IGNORECASE)
                    curr_block['to'] = blocks[-1]['to']
                    grps = grps.group(1).split(' on ')
                    curr_block['from'] = grps[0]
                    # sometimes part of the date is already here...
                    if len(grps) > 1:
                        curr_block['sent'] = grps[1]
                except AttributeError:
                    # must then be the second line?
                    grps = re.search(r"(?:on )?(.+?)-+", line['text'], flags=re.IGNORECASE)
                    # FIXME: this is not save to use, exceptions expected!
                    curr_block['sent'] = ('' if curr_block['sent'] is None else curr_block['sent']) + grps.group(1)
                    mode = 0
                continue

            # eating a header and stumbled upon next one...
            if mode >= 2 and ('-- original' in line_low or '-- forward' in line_low):
                # TODO implement
                pass

            # TODO check if all fields are filled, potentially switch to new header
            # but how to distinguish new header from previous one? could just be a long list of recipients...

            # ended up here, so it's a normal reply header
            # TODO: figure out what to do with headers missing newlines
            # TODO: keep track of what you are eating (from, to, cc, ...) and append!
            # TODO: how to deal with multi-language?
            # TODO: how to deal with broken layouts?
            curr_block['raw_header'].append(line['text'])

            # On Tue, Jan 17, 2017 at 8:14 PM, Deepak Sharma <deepakmca05@gmail.com>
            # wrote:

            # > On Jan 18, 2017 9:39 AM, "Rishabh Bhardwaj" <rbnext29@gmail.com> wrote:

            # On Fri, Mar 24, 2017 at 3:52 PM, Kadam, Gangadhar (GE Aviation, Non-GE) <
            # Gangadhar.Kadam@ge.com> wrote:
            on_match = re.search(
                r"on (?:[a-z]+, ?)?([a-z]+ \d\d?, ?\d{2,4} (?:at )?\d\d?:\d\d ?(?:am|pm)),(.+?)(?:wrote|$)",
                line['text'], flags=re.IGNORECASE)
            if on_match:
                curr_block['sent'] = on_match.group(1)
                curr_block['type'] = 'unknown'  # this kind of header exists in both cases (or does it?)
                curr_block['from'] = on_match.group(2)
                curr_block['to'] = blocks[-1]['from']
                curr_block['subject'] = blocks[-1]['subject']
                mode = 3
                continue

            # attempt eating a from line (easy catch)
            # From: Charlotte Hawkins 03/30/2000 11:33 AM
            # From:	Michael Brown/ENRON@enronXgate on 04/19/2001 05:54 PM
            line_text = line['text']
            if 'from:' in line_low:
                mode = 3
                line_text = line_text.replace('From:', '').replace('from:', '')
            elif 'to:' in line_low and 'mailto:' not in line_low:
                mode = 4
                line_text = line_text.replace('To:', '').replace('to:', '')
            elif 'cc:' in line_low:
                mode = 5
                line_text = line_text.replace('Cc:', '').replace('cc:', '')
            elif 'sent:' in line_low or 'date:' in line_low:
                mode = 6
                line_text = line_text.replace('Sent:', '').replace('sent:', '')
            elif 'subject:' in line_low:
                mode = 7
                line_text = line_text.replace('Subject:', '').replace('subject:', '')

            if mode != 6:
                # time/date info often mixed with other stuff, so try to extract it from line
                # date pattern
                # '05/30/2001', 'May 29, 2001'
                date_match = re.search(r"((?:[a-z]+ \d{1,2}, ?\d{2,4})|(?:\d{1,2}/\d{1,2}/\d{2,4}))", line_low)
                if date_match:
                    curr_block['sent'] = ('' if curr_block['sent'] is None else curr_block[
                        'sent']) + ' ' + date_match.group(1)

                # time pattern
                # '09:43:45 AM',  '7:58 AM', '04:56 PM',
                time_match = re.search(r"(\d{1,2}:\d\d(?::\d\d)?(?: ?(?:pm|am))?)", line_low)
                if time_match:
                    curr_block['sent'] = ('' if curr_block['sent'] is None else curr_block[
                        'sent']) + ' ' + time_match.group(1)

            if mode > 2:
                field = ['', '', '', 'from', 'to', 'cc', 'sent', 'subject'][mode]
                curr_block[field] = ('' if curr_block[field] is None else curr_block[field]) + ' ' + line_text
                continue

            # last resort: might just be a leading from field with no prefix
            curr_block['from'] = ('' if curr_block['from'] is None else curr_block['from']) + ' ' + line_text

            # Sara Shackleton
            # 03/01/2000 07:43 AM
            # To: Mark Taylor/HOU/ECT@ECT
            # cc: Kaye Ellis/HOU/ECT@ECT
            # Subject: Trip to Brazil

            # Shirley Crenshaw
            # 09/06/2000 12:56 PM
            # To: ludkam@aol.com
            # cc:  (bcc: Vince J Kaminski/HOU/ECT)
            # Subject: Vince's Travel Itinerary

            #  -----Original Message-----
            # From: 	Crews, David
            # Sent:	Wednesday, May 30, 2001 10:11 AM
            # To:	Buy, Rick
            # Cc:	Gorte, David
            # Subject:	RE: FYI - Project Raven

            # 	Rick Buy/ENRON@enronXgate 05/30/01 09:20 AM 	   To: David Crews/Enron Communications@Enron Communications  cc: David Gorte/ENRON@enronXgate  Subject: RE: FYI - Project Raven

            #     -----Original Message-----
            #    From:   jennifer.d.sanders@us.andersen.com@ENRON
            #
            # [mailto:IMCEANOTES-jennifer+2Ed+2Esanders+40us+2Eandersen+2Ecom+40ENRON@ENRON.com]
            #
            #
            #
            #    Sent:   Tuesday, August 07, 2001 10:58 AM
            #    To:     Nemec, Gerald
            #    Subject:  Re: Hello!

            # To:   IMCEANOTES-jennifer+2Esanders/40us/2Eandersen/2Ecom/40ENRON@enron.com
            # cc:     (bcc: Jennifer D. Sanders)
            # Date: 08/07/2001 03:09 PM
            # From: Gerald.Nemec@enron.com
            # Subject:  RE: Hello!
        else:
            mode = 0
            curr_block['text'].append(line['text'])

    blocks.append(curr_block)
    # blockss.append({
    #     'blocks': blocks,
    #     'text': si,
    #     'predictions': pred
    # })

    # join lines
    blocks = [{'raw_header': '\n'.join(part['raw_header']), 'text': '\n'.join(part['text'])} for part in blocks]

    # transform to same format as regex result
    blocks = [(part['raw_header'], part['text']) for part in blocks if part['raw_header']]

    return blocks


def get_body(raw_email):
    """Actual method that extracts and returns the body of an email."""
    text_lines = raw_email.splitlines()

    func_b = enron_two_zone_line_b_func
    model = enron_two_zone_model
    # embedding_b = enron_two_zone_line_b

    text_embedded = _embed(text_lines, [func_b])
    head_body_predictions = model.predict(np.array([text_embedded])).tolist()[0]

    head_body_indicator = list(zip(text_lines, head_body_predictions))

    body_text = ''
    for i in range(0, len(head_body_indicator)):
        if int(head_body_indicator[i][1][0]) == int(1.0):
            body_text += head_body_indicator[i][0] + '\n'

    return body_text


def get_headers(raw_email):
    """Actual method that extracts and returns the head of an email."""
    text_lines = raw_email.splitlines()

    func_b = enron_two_zone_line_b_func
    model = enron_two_zone_model
    # embedding_b = enron_two_zone_line_b

    text_embedded = _embed(text_lines, [func_b])
    head_body_predictions = model.predict(np.array([text_embedded])).tolist()[0]

    head_body_indicator = list(zip(text_lines, head_body_predictions))

    headers = {}
    head_count = 0
    at_head = False

    for i in range(0, len(head_body_indicator)):
        if int(head_body_indicator[i][1][0]) == int(0.0):
            if not at_head:
                at_head = True
                head_count += 1
                headers[head_count] = ''

            headers[head_count] += head_body_indicator[i][0] + '\n'
        else:
            at_head = False

    return headers
