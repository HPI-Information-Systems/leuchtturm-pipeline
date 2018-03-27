"""Test preprocessing module."""

import json

from src.preprocessing import HeaderParsing, TextCleaning, LanguageDetection, EmailSplitting


em = 'Message-ID: <26905545.1075861107045.JavaMail.evans@thyme>\r\nDate: Fri, 15 Feb 2002 09:07:17 -0800 (PST)\r\nFrom: susan.bailey@enron.com\r\nTo: sara.shackleton@enron.com\r\nSubject: RE: Thiele Kaolin Company\r\nMime-Version: 1.0\r\nContent-Type: text/plain; charset=us-ascii\r\nContent-Transfer-Encoding: 7bit\r\nX-From: Bailey, Susan </O=ENRON/OU=NA/CN=RECIPIENTS/CN=SBAILE2>\r\nX-To: Shackleton, Sara </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Sshackl>\r\nX-cc: \r\nX-bcc: \r\nX-Folder: \\Susan_Bailey_Mar2002\\Bailey, Susan\\Deleted Items\r\nX-Origin: Bailey-S\r\nX-FileName: sbaile2 (Non-Privileged).pst\r\n\r\n\nSara,\n\nAs in our past exercises for the \"Non-Terminated/In-the-Money\" Counterparties, the Thiele-Kaolin Company continues to appear as a \"financial\" trading relationship.\n\nHowever, according to the Confirmation Desk there are only physical arrangements with this counterparty (as evidenced by our GTC docs).  I hope that Credit can resolve the mystery???\n\nSusan  \n -----Original Message-----\nFrom: \tShackleton, Sara  \nSent:\tFriday, February 15, 2002 10:21 AM\nTo:\tMcMichael Jr., Ed; Dicarlo, Louis; Moran, Tom; Apollo, Beth\nCc:\tBailey, Susan; Boyd, Samantha; Panus, Stephanie\nSubject:\tThiele Kaolin Company\n\nWe cannot locate any financial contracts for this counterparty.\n\nPlease verify that this party belongs on the ITM Non-terminated financial gas list.\n\nSara Shackleton\nEnron Wholesale Services\n1400 Smith Street, EB3801a\nHouston, TX  77002\nPh:  (713) 853-5620\nFax: (713) 646-3490'  # NOQA

doc = json.dumps({'raw': em, 'doc_id': '1'})


def test_header_parsing():
    """Header fields are present."""
    result = json.loads(HeaderParsing().run_on_document(doc))
    assert 'header' in result
    assert 'susan.bailey@enron.com' == result['header']['sender']['email']
    assert 'RE: Thiele Kaolin Company' == result['header']['subject']
    assert 'sara.shackleton@enron.com' == result['header']['recipients'][0]['email']


def test_header_body_separation():
    """Main header is not in body part."""
    result = json.loads(HeaderParsing().run_on_document(doc))
    assert 'body' in result
    assert 'Message-ID: <26905545.1075861107045.JavaMail.evans@thyme>' not in result['body']
    assert 'As in our past exercises for the' in result['body']


def test_text_cleaning():
    """Text cleaning doesn't leave unnormalized whitespace."""
    text = json.dumps({'body': '\n\n\n\n\r\rThis is awesome!!!!\n\n\nGreat!'})
    result = json.loads(TextCleaning().run_on_document(text))
    assert 'text_clean' in result
    assert 'This is awesome!!!!\nGreat!' == result['text_clean']


def test_language_detection_en():
    """Test lang detect on english text."""
    text = json.dumps({'body': 'I am a text written in a popular language!'})
    result = json.loads(LanguageDetection(read_from='body').run_on_document(text))
    assert 'lang' in result
    assert 'en' == result['lang']


def test_language_detection_de():
    """Test lang detect on weird text."""
    text = json.dumps({'body': 'Ich bin ein Text in einer beliebten Sprache!'})
    result = json.loads(LanguageDetection(read_from='body').run_on_document(text))
    assert 'lang' in result
    assert 'de' == result['lang']


def test_email_splitting():
    """Email is plit into conversational parts."""
    result = EmailSplitting().run_on_document(doc)
    assert len(result) == 2
