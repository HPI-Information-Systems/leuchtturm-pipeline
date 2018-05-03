"""Feature engineering."""

from sklearn.preprocessing import LabelEncoder
import pandas as pd


_integer_columns = ['length_of_body', 'amount_of_recipients', 'amount_of_cc_recipients']
_float_columns = []

_one_hot_encode = []
_label_encode = []

_drop = ['email', 'date', 'subject', 'body', 'from_name', 'from_email', 'to_names', 'to_emails', 'cc_names', 'cc_emails', 'bcc_names', 'bcc_emails', 'reply_to_name', 'reply_to_email']


def add_all_features(df):
    df['length_of_body'] = add_length_of_body(df)
    df['amount_of_recipients'] = add_amount_of('to_emails', df)
    df['amount_of_cc_recipients'] = add_amount_of('cc_emails',df)

    return df


def transform_features(df):
    global _integer_columns
    global _float_columns
    global _one_hot_encode
    global _label_encode

    # ensure correct data types
    for column in _integer_columns:
        df[column] = df[column].astype(int)

    for column in _float_columns:
        df[column] = df[column].astype(float)

    # one-hot-encode
    df = pd.get_dummies(data=df, prefix=_one_hot_encode, columns=_one_hot_encode)

    # label-encode
    for column in _label_encode:
        encoder = LabelEncoder().fit(df[column])
        df[column] = encoder.transform(df[column])

    return df


def drop_features(df, drop=[]):
    # append columns that are always supposed to be dropped (defined in _drop, see above)
    drop = drop + _drop

    # check if columns are stil in the dataFrame
    data_columns = list(df)
    drop_hot_encoded = []
    for to_be_dropped in drop:
        drop_hot_encoded = drop_hot_encoded + [column for column in data_columns if to_be_dropped in column]

    # drop columns that may not be needed any more
    df.drop(drop_hot_encoded, inplace=True, axis=1)

    return df


# ----------- FEATURE GENERATION -----------


def add_length_of_body(df):
    return [len(body) for body in df['body']]


def add_amount_of(field_name, df):
    return [len(field.split(',')) for field in df[field_name]]


def add_vectorized_field(df, field, new_field, vectorizer):
    matrix = vectorizer.transform(df[field]).toarray()
    labels = [new_field + '_' + str(i) for i in range(0, len(matrix[0]))]
    df_matrix = pd.DataFrame(data=matrix, columns=labels)

    return pd.concat([df, df_matrix], axis=1)
