# flake8: noqa
"""Test preprocessing module."""

from src.correspondent_extraction_aggregation import CorrespondentDataExtraction, CorrespondentDataAggregation
from .data import shackleton_emails, aggregated_shackleton_correspondent_object
import json
from config.config import Config

conf = Config(['-c', 'config/testconfig.ini'])

signature_email_1 = {
    'text_clean': "Gil:\nBefore any brokerage account can be opened, you need to advise the RAC group. Please send an email to the three persons (\"Cc\") above explaining what accounts Enron North America Corp. (\"ENA\") requires and the purpose of those accounts (if you have not already done so). ENA has a brokerage agreement with Bear Stearns Compamies, Inc. but not with Bear Stearns International Limited. ENA already has resolutions for opening brokerage accounts.\nSara\nEnron Wholesale Services\n1400 Smith Street, EB3801a\nHouston, TX 77002\nPh: (777) 777-7777\nFax: (713) 646-3490",
    'sender_email_address': 'sara.shackleton@enron.com',
    'signature': "Sara\nEnron Wholesale Services\n1400 Smith Street, EB3801a\nHouston, TX 77002\nPh: (777) 777-7777\nFax: (713) 646-3490"
}

mcdermott_email = {
    'text_clean': "Cheryl:\nAttached are Welch's comments.\nAs you already know, Jennifer is doing the 2-month deal (but no others until the ISDA is inked). We do have to investigate the coop issue under Michigan law. Both the swap CP and the parent have the word \"cooperative\" in their names. Please see Lotus Notes. You should send an immediate email to the department to see if anyone has used Michigan counsel (I saw a single reference in the Jurisdictional Database). We may be able to obtain an opinion from McDermott or even Welch's inhouse counsel.\nSara Shackleton\nEnron North America Corp.\n1400 Smith Street, EB 3801a\nHouston, Texas 77002\n713-853-5620 (phone)\n713-646-3490 (fax)\nsara.shackleton@enron.com\n----- Forwarded by Sara Shackleton/HOU/ECT on 05/24/2001 03:42 PM -----\n dkoya@mwe.com\n 05/24/2001 01:29 PM\n To: sara.shackleton@enron.com\n cc: tbockhorst@welchs.com, gbenson@mwe.com\n Subject: Welch's / Enron - Markup of schedule and paragraph 13\nSara,\nAttached are Welch's comments to the schedule and paragraph 13 proposed by\nEnron. I look forward to discussing the terms of these documents at your\nearliest convenience. The form of the Letter of Credit and the guaranty are\nstill under review by Welch's and MWE.\nThanks,\nDevi S. Koya\nMcDermott, Will & Emery\n227 West Monroe\nChicago, IL 60606\nPh: 312-984-2133\nFax: 312-984-7700\n(See attached file: CHI99_3716524_1.DOC)\n(See attached file: CHI99_3719227_1.DOC)\n*******\nFor more information on McDERMOTT, WILL & EMERY please visit our website at:\nhttp://www.mwe.com/\n - CHI99_3716524_1.DOC\n - CHI99_3719227_1.DOC",
    'sender_email_address': 'sara.shackleton@enron.com',
    'signature': "Thanks,\nDevi S. Koya\nMcDermott, Will & Emery\n227 West Monroe\nChicago, IL 60606\nPh: 312-984-2133\nFax: 312-984-7700"
}

phone_numbers_1 = {
    'text_clean': "We just have been asked to review on an expedited basis all material contracts of ENE and ENA for change-of-control type provisions. Please let us know what contracts or guidance you may have or know of; we need to begin reviewing/collecting information ASAP, i.e., this weekend. If you know of someone who should receive this e-mail but who has not, please forward. You may contact me at any of the below numbers. Thanks for your help.\nRegards, NJD\nNora J. Dobin\nSenior Counsel\nEnron Corp. - Global Finance\n1400 Smith Street, Suite 2083\nHouston, Texas 77002\nPhone: 713/345-7723\nFax: 713/853-9252\nHome: 713/864-1175",
    'sender_email_address': 'nora.dobin@enron.com',
    'signature': "Regards, NJD\nNora J. Dobin\nSenior Counsel\nEnron Corp. - Global Finance\n1400 Smith Street, Suite 2083\nHouston, Texas 77002\nPhone: 713/345-7723\nFax: 713/853-9252\nHome: 713/864-1175"
}

phone_numbers_email_addresses = {
    'text_clean': "You are sending biz e-mails at 7:30 on Saturday morning? You must have kids---at any rate, thanks for the note. Am glad things worked out well and that you were so flexible re: scheduling---let me know if you need anything going forward (as I assume you will be)---and if I do not talk to you, (try to) have a terrific holiday! mb\nMarcy Brinegar\nMBM Consultants\n(770) 886-6202 (Office)\n(678) 234-7644 (Cell)\n(770) 886-8202 (fax)\nbrinconsult@aol.com\nmbmconsult@adelphia.net",
    'sender_email_address': 'brinconsult@aol.com',
    'signature': "Marcy Brinegar\nMBM Consultants\n(770) 886-6202 (Office)\n(678) 234-7644 (Cell)\n(770) 886-8202 (fax)\nbrinconsult@aol.com\nmbmconsult@adelphia.net"
}

two_aliases = {
    'sender_email_address': 'dana@gablegroup.com',
    'signature': "Thanks,\nDana\nDana Perino\ndana@gablegroup.com\n619-234-1300 ext. 238"
}


def test_extract_phone_numbers_from_signature_1():
    phone_numbers = CorrespondentDataExtraction(conf).extract_phone_numbers_from(signature_email_1['signature'])
    assert phone_numbers['phone_numbers_office'] == ['(777) 777-7777']
    assert phone_numbers['phone_numbers_cell'] == []
    assert phone_numbers['phone_numbers_fax'] == ['(713) 646-3490']
    assert phone_numbers['phone_numbers_home'] == []


def test_extract_phone_numbers_from_signature_2():
    phone_numbers = CorrespondentDataExtraction(conf).extract_phone_numbers_from(mcdermott_email['signature'])
    assert phone_numbers['phone_numbers_office'] == ['312-984-2133']
    assert phone_numbers['phone_numbers_cell'] == []
    assert phone_numbers['phone_numbers_fax'] == ['312-984-7700']
    assert phone_numbers['phone_numbers_home'] == []


def test_extract_phone_numbers_from_signature_3():
    phone_numbers = CorrespondentDataExtraction(conf).extract_phone_numbers_from(phone_numbers_1['signature'])
    assert phone_numbers['phone_numbers_office'] == ['713/345-7723']
    assert phone_numbers['phone_numbers_cell'] == []
    assert phone_numbers['phone_numbers_fax'] == ['713/853-9252']
    assert phone_numbers['phone_numbers_home'] == ['713/864-1175']


def test_extract_phone_numbers_from_signature_4():
    phone_numbers = CorrespondentDataExtraction(conf).extract_phone_numbers_from(phone_numbers_email_addresses['signature'])
    assert phone_numbers['phone_numbers_office'] == ['(770) 886-6202']
    assert phone_numbers['phone_numbers_cell'] == ['(678) 234-7644']
    assert phone_numbers['phone_numbers_fax'] == ['(770) 886-8202']
    assert phone_numbers['phone_numbers_home'] == []


def test_extract_email_addresses_from_signature():
    email_addresses = CorrespondentDataExtraction(conf).extract_email_addresses_from(
        phone_numbers_email_addresses['signature']
    )
    assert email_addresses == ['brinconsult@aol.com', 'mbmconsult@adelphia.net']


def test_extract_aliases_from_signature():
    emails_with_aliases_in_signature = [
        signature_email_1,
        phone_numbers_1,
        two_aliases
    ]
    aliases = [
        {'Sara'},
        {'Nora J. Dobin'},
        {'Dana', 'Dana Perino'}
    ]
    correspondentDataExtraction = CorrespondentDataExtraction(conf)
    for i, email in enumerate(emails_with_aliases_in_signature):
        assert aliases[i] == set(correspondentDataExtraction.extract_aliases_from(
            email['signature'],
            email['sender_email_address'][:3]
        ))


correspondent_object_keys = {
    'aliases_from_signature', 'aliases', 'phone_numbers_cell', 'phone_numbers_fax', 'phone_numbers_office',
    'phone_numbers_home', 'signatures', 'writes_to', 'email_addresses_from_signature',
    'email_addresses', 'identifying_names', 'source_count'
}


def test_preparation_for_aggregation():
    for email in shackleton_emails:
        email = email.copy()
        prepared_email = CorrespondentDataExtraction(conf).convert_and_rename_fields(email)
        prepared_email = CorrespondentDataAggregation(conf).prepare_for_reduction(json.dumps(prepared_email))
        prepared_email = json.loads(prepared_email)
        keys = prepared_email.keys()
        assert set(keys) == correspondent_object_keys
        for key in keys:
            if key == 'identifying_names':
                assert type(prepared_email[key]) == dict
            elif key == 'source_count':
                assert type(prepared_email[key]) == int
            else:
                assert type(prepared_email[key]) == list


def test_aggregation_of_correspondent_data():
    prepared_emails = []

    for email in shackleton_emails:
        email = email.copy()
        prepared_email = CorrespondentDataExtraction(conf).convert_and_rename_fields(email)
        prepared_email = CorrespondentDataAggregation(conf).prepare_for_reduction(json.dumps(prepared_email))
        prepared_emails.append(prepared_email)

    aggregated_correspondent = CorrespondentDataAggregation(conf).merge_correspondents_by_email_address(*prepared_emails)
    aggregated_correspondent = json.loads(aggregated_correspondent)
    keys = aggregated_correspondent.keys()

    assert set(keys) == correspondent_object_keys

    for key in keys:
        if key != 'source_count':
            assert set(aggregated_correspondent[key]) == set(aggregated_shackleton_correspondent_object[key])
        else:
            assert aggregated_correspondent[key] == aggregated_shackleton_correspondent_object[key]
