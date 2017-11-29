import keras.backend as K
from keras_contrib.utils import save_load_utils
from keras.models import model_from_json
from keras.models import Model
from keras.layers import Masking, GRU, Input, Bidirectional
from keras_contrib.layers import CRF
# from flask import Flask, request
import numpy as np
from sklearn.preprocessing import LabelEncoder
import os.path
# from flask.json import jsonify
from pyspark import SparkContext


line_embedding_size = 32

# this is NN stuff to analyze the mail


def load_keras_model(path, model=None):
    with open(os.path.abspath(path + '.json'), 'r') as jf:
        json_model = jf.read()
    if model is None:
        model = model_from_json(json_model)
    try:
        save_load_utils.load_all_weights(model, os.path.abspath(path + '.hdf5'))
    except KeyError:
        model.load_weights(os.path.abspath(path + '.hdf5'))
    return model


def get_mail_model_two():
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


def get_embedding_function(model):
    model_in = [model.input]
    embedding_func = K.function(model_in + [K.learning_phase()], [model.layers[-2].output])

    def lambdo(x):
        return embedding_func([x, 0.])[0]

    return lambdo


enron_two_zone_line_b = load_keras_model('./emailbody/models/two_zones/enron_line_model_b')
enron_two_zone_model = load_keras_model('./emailbody/models/two_zones/enron_model', model=get_mail_model_two())
asf_two_zone_line_b = load_keras_model('./emailbody/models/two_zones/asf_line_model_b')
asf_two_zone_model = load_keras_model('./emailbody/models/two_zones/asf_model', model=get_mail_model_two())
enron_two_zone_line_b_func = get_embedding_function(enron_two_zone_line_b)
asf_two_zone_line_b_func = get_embedding_function(asf_two_zone_line_b)

two_encoder = LabelEncoder().fit(['Body', 'Header'])

char_index = list(' '
                  'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                  'abcdefghijklmnopqrstuvwxyz'
                  '0123456789'
                  '@€-_.:,;#\'+*~\?}=])[({/&%$§"!^°|><´`\n')
num_possible_chars = len(char_index)
line_length = 80


def embed(lines, embedding_functions=None):
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


def getBody(sourcepath):
    """The actual method that extracts and returns the body of an email."""

    mail_text = ''

    with open(sourcepath, 'r') as mail:
        mail_text = mail.read()

    text_lines = mail_text.splitlines()

    func_b = enron_two_zone_line_b_func
    model = enron_two_zone_model
    # embedding_b = enron_two_zone_line_b

    text_embedded = embed(text_lines, [func_b])
    head_body_predictions = model.predict(np.array([text_embedded])).tolist()[0]

    head_body_indicator = list(zip(text_lines, head_body_predictions))

    body_text = ''
    for i in range(0, len(head_body_indicator)):
        if int(head_body_indicator[i][1][0]) == int(1.0):
            body_text += head_body_indicator[i][0]

    return body_text


def getBodyParallel(sourcepath):
    """A currently unused method that shows how to split a huge file of mails into lines
    to perform body extraction on."""

    sc = SparkContext()

    data = sc.textFile(sourcepath)

    print(data)

    bodies = data.map(lambda mail: extractBody(mail)).collect()

    print(bodies)

    sc.stop()

    def extractBody(string):
        text_lines = string.splitlines()

        func_b = enron_two_zone_line_b_func
        model = enron_two_zone_model
        # embedding_b = enron_two_zone_line_b

        text_embedded = embed(text_lines, [func_b])
        head_body_predictions = model.predict(np.array([text_embedded])).tolist()[0]

        head_body_indicator = list(zip(text_lines, head_body_predictions))

        body_text = ''
        for i in range(0, len(head_body_indicator)):
            if int(head_body_indicator[i][1][0]) == int(1.0):
                body_text += head_body_indicator[i][0]

        return body_text


