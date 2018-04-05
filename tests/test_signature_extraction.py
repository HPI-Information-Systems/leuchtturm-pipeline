"""Test preprocessing module."""

from src.signature_extraction import SignatureExtraction
import talon

attachment_notice_email_1 = {
    'text_clean': "Eric asked that I forward this to you both.\nSusan\n-----Original Message-----\nFrom: Ilnycky, Andrew [mailto:andy.ilnycky@foothillspipe.com]\nSent: Friday, December 21, 2001 4:33 PM\nTo: 'dpentzien@nisource.com'; 'bdreese@duke-energy.com';\n'skschroeder@duke-energy.com'; 'jay.holm@elpaso.com';\n'byron.wright@elpaso.com'; Gadd, Eric; Reinecke, Bryan; Cohen, Rob;\nHeeg, Marty; Ilnycky, Andrew; Hill, Robert; 'Peter.Lund@neg.pge.com';\n'lauri.newton@neg.pge.com'; 'dave.vandriel@neg.pge.com';\n'spatrick@sempra.com'; 'wpurcell@sempra-intl.com';\n'vincent_lee@transcanada.com'; McConaghy Dennis;\n'tony_palmer@transcanada.com'; 'mbirch@wei.org'; 'bscott@wei.org';\n'jtaylor@wei.org'; 'cavan.c.carlton@williams.com';\n'jim.c.moore@williams.com'; 'peter.c.thomas@williams.com'\nCc: Ellwood, John\nSubject: ANGTS Proposal - Executive Summary\nAttached we are forwarding electronic copies of the ANGTS Proposal and cover\nletter sent via courier to Phillips, BP and Exxon Mobil. A hard copy was\nalso couriered to ANGTS members.\nPlease accept my best Season's Greetings.\nAndrew M. Ilnycky\nManager, Commercial and Customer Relations\nFoothills Pipe Lines Ltd.\nTel: 403-294-4434\nFax: 403-294-4174\n <<Cover Letter.doc>> <<Final Executive Summary.doc>>",
    'sender_email_address': 'eric.gadd@enron.com'
}

attachment_notice_email_2 = {
    'text_clean': "For your info- I will be having a tailgate party both pre and post game, regardless of outcome. It is located at 9th and S on the east side of 9th street towards the southwest end of the lot where there is a large billboard advertising a 30$$ ++ Jackpot ( at least that's what it had on last weekend) Please feel free to have the group join in if you get there in enough time. Otherwise I will see you all at the game around 10:30. Mary Kay\n -----Original Message-----\nFrom: Armstrong, Julie Sent: Tuesday, October 23, 2001 5:12 PM\nTo: 'clark.smith@elpaso.com'; 'tmcgill@nisource.com'; Hayes, Robert; McCarty, Danny; Neubauer, Dave; Miller, Mary Kay\nCc: Stark, Cindy; 'shea.mollere@elpaso.com'; 'pmcleod@nisource.com'; Kovalcik, Tammy; Call, Josie; Cappiello, Deborah\nSubject: October 27th Husker Game\nAttached please find the travel itinerary for Saturday, October 27th, the Husker's game. Please note the departure time of 7:00 am from Houston/Enron Aviation. A map/directions to the Enron hanger has been attached.\nPlease call me at 713-853-3597 if you have any questions. Have a great time.\n << File: October27Huskergame.doc >> << File: Aviationmap.jpg >> Deb:\nPlease forward the contact and email information on Dennis Stell and Tim Christensen to me so I can forward the above information. Thank you.",
    'sender_email_address': 'kay.miller@enron.com'
}

standard_signature_emails = [
    {  # blackberry
        'text_clean': "I have been asked to provide a statement to Gov Knowles on Friday morning about Enron's siituation and the effect on the Alaska project. Advice?\n--------------------------\nSent from my BlackBerry Wireless Handheld (www.BlackBerry.net)",
        'sender_email_address': 'eric.gadd@enron.com'
    },
    {  # hotmail
        'text_clean': "I did not bring my stuff. Maybe tomarrow. yes, i do not like the cold. Besides I want to go when its warm so the view will be more interesting on the female joggers.\n_________________________________________________________________\nJoin the world's largest e-mail service with MSN Hotmail. \nhttp://www.hotmail.com",
        'sender_email_address': 'erwollam@hotmail.com'
    },
    {  # yahoo
        'text_clean': "cocksucker\n__________________________________________________\nDo You Yahoo!?\nSend your FREE holiday greetings online!\nhttp://greetings.yahoo.com",
        'sender_email_address': 'jeffreyskilling@yahoo.com'
    }
]

mcdermott_email = {
    'text_clean': "Cheryl:\nAttached are Welch's comments.\nAs you already know, Jennifer is doing the 2-month deal (but no others until the ISDA is inked). We do have to investigate the coop issue under Michigan law. Both the swap CP and the parent have the word \"cooperative\" in their names. Please see Lotus Notes. You should send an immediate email to the department to see if anyone has used Michigan counsel (I saw a single reference in the Jurisdictional Database). We may be able to obtain an opinion from McDermott or even Welch's inhouse counsel.\nSara Shackleton\nEnron North America Corp.\n1400 Smith Street, EB 3801a\nHouston, Texas 77002\n713-853-5620 (phone)\n713-646-3490 (fax)\nsara.shackleton@enron.com\n----- Forwarded by Sara Shackleton/HOU/ECT on 05/24/2001 03:42 PM -----\n dkoya@mwe.com\n 05/24/2001 01:29 PM\n To: sara.shackleton@enron.com\n cc: tbockhorst@welchs.com, gbenson@mwe.com\n Subject: Welch's / Enron - Markup of schedule and paragraph 13\nSara,\nAttached are Welch's comments to the schedule and paragraph 13 proposed by\nEnron. I look forward to discussing the terms of these documents at your\nearliest convenience. The form of the Letter of Credit and the guaranty are\nstill under review by Welch's and MWE.\nThanks,\nDevi S. Koya\nMcDermott, Will & Emery\n227 West Monroe\nChicago, IL 60606\nPh: 312-984-2133\nFax: 312-984-7700\n(See attached file: CHI99_3716524_1.DOC)\n(See attached file: CHI99_3719227_1.DOC)\n*******\nFor more information on McDERMOTT, WILL & EMERY please visit our website at:\nhttp://www.mwe.com/\n - CHI99_3716524_1.DOC\n - CHI99_3719227_1.DOC",
    'sender_email_address': 'sara.shackleton@enron.com',
    'signature': "Thanks,\nDevi S. Koya\nMcDermott, Will & Emery\n227 West Monroe\nChicago, IL 60606\nPh: 312-984-2133\nFax: 312-984-7700"
}

signature_email_1 = {
    'text_clean': "Gil:\nBefore any brokerage account can be opened, you need to advise the RAC group. Please send an email to the three persons (\"Cc\") above explaining what accounts Enron North America Corp. (\"ENA\") requires and the purpose of those accounts (if you have not already done so). ENA has a brokerage agreement with Bear Stearns Compamies, Inc. but not with Bear Stearns International Limited. ENA already has resolutions for opening brokerage accounts.\nSara\nEnron Wholesale Services\n1400 Smith Street, EB3801a\nHouston, TX 77002\nPh: (777) 777-7777\nFax: (713) 646-3490",
    'sender_email_address': 'sara.shackleton@enron.com',
    'signature': "Sara\nEnron Wholesale Services\n1400 Smith Street, EB3801a\nHouston, TX 77002\nPh: (777) 777-7777\nFax: (713) 646-3490"
}


def test_remove_attachment_notices_1():
    body_without_signature = SignatureExtraction().remove_attachment_notices(attachment_notice_email_1['text_clean'])
    assert body_without_signature.strip() == "Eric asked that I forward this to you both.\nSusan\n-----Original Message-----\nFrom: Ilnycky, Andrew [mailto:andy.ilnycky@foothillspipe.com]\nSent: Friday, December 21, 2001 4:33 PM\nTo: 'dpentzien@nisource.com'; 'bdreese@duke-energy.com';\n'skschroeder@duke-energy.com'; 'jay.holm@elpaso.com';\n'byron.wright@elpaso.com'; Gadd, Eric; Reinecke, Bryan; Cohen, Rob;\nHeeg, Marty; Ilnycky, Andrew; Hill, Robert; 'Peter.Lund@neg.pge.com';\n'lauri.newton@neg.pge.com'; 'dave.vandriel@neg.pge.com';\n'spatrick@sempra.com'; 'wpurcell@sempra-intl.com';\n'vincent_lee@transcanada.com'; McConaghy Dennis;\n'tony_palmer@transcanada.com'; 'mbirch@wei.org'; 'bscott@wei.org';\n'jtaylor@wei.org'; 'cavan.c.carlton@williams.com';\n'jim.c.moore@williams.com'; 'peter.c.thomas@williams.com'\nCc: Ellwood, John\nSubject: ANGTS Proposal - Executive Summary\nAttached we are forwarding electronic copies of the ANGTS Proposal and cover\nletter sent via courier to Phillips, BP and Exxon Mobil. A hard copy was\nalso couriered to ANGTS members.\nPlease accept my best Season's Greetings.\nAndrew M. Ilnycky\nManager, Commercial and Customer Relations\nFoothills Pipe Lines Ltd.\nTel: 403-294-4434\nFax: 403-294-4174"

def test_remove_attachment_notices_2():
    body_without_signature = SignatureExtraction().remove_attachment_notices(attachment_notice_email_2['text_clean'])
    assert body_without_signature.strip() == "For your info- I will be having a tailgate party both pre and post game, regardless of outcome. It is located at 9th and S on the east side of 9th street towards the southwest end of the lot where there is a large billboard advertising a 30$$ ++ Jackpot ( at least that's what it had on last weekend) Please feel free to have the group join in if you get there in enough time. Otherwise I will see you all at the game around 10:30. Mary Kay\n -----Original Message-----\nFrom: Armstrong, Julie Sent: Tuesday, October 23, 2001 5:12 PM\nTo: 'clark.smith@elpaso.com'; 'tmcgill@nisource.com'; Hayes, Robert; McCarty, Danny; Neubauer, Dave; Miller, Mary Kay\nCc: Stark, Cindy; 'shea.mollere@elpaso.com'; 'pmcleod@nisource.com'; Kovalcik, Tammy; Call, Josie; Cappiello, Deborah\nSubject: October 27th Husker Game\nAttached please find the travel itinerary for Saturday, October 27th, the Husker's game. Please note the departure time of 7:00 am from Houston/Enron Aviation. A map/directions to the Enron hanger has been attached.\nPlease call me at 713-853-3597 if you have any questions. Have a great time.\n \nDeb:\nPlease forward the contact and email information on Dennis Stell and Tim Christensen to me so I can forward the above information. Thank you."

def test_remove_standard_signatures_blackberry():
    body_without_signature, sent_from_mobile = SignatureExtraction().remove_standard_signatures(standard_signature_emails[0]['text_clean'])
    assert body_without_signature.strip() == "I have been asked to provide a statement to Gov Knowles on Friday morning about Enron's siituation and the effect on the Alaska project. Advice?"
    assert sent_from_mobile

def test_remove_standard_signatures_hotmail():
    body_without_signature, sent_from_mobile = SignatureExtraction().remove_standard_signatures(standard_signature_emails[1]['text_clean'])
    assert body_without_signature.strip() == "I did not bring my stuff. Maybe tomarrow. yes, i do not like the cold. Besides I want to go when its warm so the view will be more interesting on the female joggers."
    assert not sent_from_mobile

def test_remove_standard_signatures_yahoo():
    body_without_signature, sent_from_mobile = SignatureExtraction().remove_standard_signatures(standard_signature_emails[2]['text_clean'])
    assert body_without_signature.strip() == "cocksucker"
    assert not sent_from_mobile

def test_remove_attachment_notices_remove_standard_signatures():
    body_without_signature = SignatureExtraction().remove_attachment_notices(mcdermott_email['text_clean'])
    body_without_signature, sent_from_mobile = SignatureExtraction().remove_standard_signatures(body_without_signature)
    assert body_without_signature.strip() == "Cheryl:\nAttached are Welch's comments.\nAs you already know, Jennifer is doing the 2-month deal (but no others until the ISDA is inked). We do have to investigate the coop issue under Michigan law. Both the swap CP and the parent have the word \"cooperative\" in their names. Please see Lotus Notes. You should send an immediate email to the department to see if anyone has used Michigan counsel (I saw a single reference in the Jurisdictional Database). We may be able to obtain an opinion from McDermott or even Welch's inhouse counsel.\nSara Shackleton\nEnron North America Corp.\n1400 Smith Street, EB 3801a\nHouston, Texas 77002\n713-853-5620 (phone)\n713-646-3490 (fax)\nsara.shackleton@enron.com\n----- Forwarded by Sara Shackleton/HOU/ECT on 05/24/2001 03:42 PM -----\n dkoya@mwe.com\n 05/24/2001 01:29 PM\n To: sara.shackleton@enron.com\n cc: tbockhorst@welchs.com, gbenson@mwe.com\n Subject: Welch's / Enron - Markup of schedule and paragraph 13\nSara,\nAttached are Welch's comments to the schedule and paragraph 13 proposed by\nEnron. I look forward to discussing the terms of these documents at your\nearliest convenience. The form of the Letter of Credit and the guaranty are\nstill under review by Welch's and MWE.\nThanks,\nDevi S. Koya\nMcDermott, Will & Emery\n227 West Monroe\nChicago, IL 60606\nPh: 312-984-2133\nFax: 312-984-7700"
    assert not sent_from_mobile

def test_remove_attachment_notices_remove_standard_signatures_extract_signature():
    body_without_signature = SignatureExtraction().remove_attachment_notices(mcdermott_email['text_clean'])
    body_without_signature, sent_from_mobile = SignatureExtraction().remove_standard_signatures(body_without_signature)
    assert body_without_signature.strip() == "Cheryl:\nAttached are Welch's comments.\nAs you already know, Jennifer is doing the 2-month deal (but no others until the ISDA is inked). We do have to investigate the coop issue under Michigan law. Both the swap CP and the parent have the word \"cooperative\" in their names. Please see Lotus Notes. You should send an immediate email to the department to see if anyone has used Michigan counsel (I saw a single reference in the Jurisdictional Database). We may be able to obtain an opinion from McDermott or even Welch's inhouse counsel.\nSara Shackleton\nEnron North America Corp.\n1400 Smith Street, EB 3801a\nHouston, Texas 77002\n713-853-5620 (phone)\n713-646-3490 (fax)\nsara.shackleton@enron.com\n----- Forwarded by Sara Shackleton/HOU/ECT on 05/24/2001 03:42 PM -----\n dkoya@mwe.com\n 05/24/2001 01:29 PM\n To: sara.shackleton@enron.com\n cc: tbockhorst@welchs.com, gbenson@mwe.com\n Subject: Welch's / Enron - Markup of schedule and paragraph 13\nSara,\nAttached are Welch's comments to the schedule and paragraph 13 proposed by\nEnron. I look forward to discussing the terms of these documents at your\nearliest convenience. The form of the Letter of Credit and the guaranty are\nstill under review by Welch's and MWE.\nThanks,\nDevi S. Koya\nMcDermott, Will & Emery\n227 West Monroe\nChicago, IL 60606\nPh: 312-984-2133\nFax: 312-984-7700"
    assert not sent_from_mobile
    talon.init()
    body_without_signature, signature = SignatureExtraction().extract_signature(body_without_signature, mcdermott_email['sender_email_address'])
    assert body_without_signature.strip() == "Cheryl:\nAttached are Welch's comments.\nAs you already know, Jennifer is doing the 2-month deal (but no others until the ISDA is inked). We do have to investigate the coop issue under Michigan law. Both the swap CP and the parent have the word \"cooperative\" in their names. Please see Lotus Notes. You should send an immediate email to the department to see if anyone has used Michigan counsel (I saw a single reference in the Jurisdictional Database). We may be able to obtain an opinion from McDermott or even Welch's inhouse counsel.\nSara Shackleton\nEnron North America Corp.\n1400 Smith Street, EB 3801a\nHouston, Texas 77002\n713-853-5620 (phone)\n713-646-3490 (fax)\nsara.shackleton@enron.com\n----- Forwarded by Sara Shackleton/HOU/ECT on 05/24/2001 03:42 PM -----\n dkoya@mwe.com\n 05/24/2001 01:29 PM\n To: sara.shackleton@enron.com\n cc: tbockhorst@welchs.com, gbenson@mwe.com\n Subject: Welch's / Enron - Markup of schedule and paragraph 13\nSara,\nAttached are Welch's comments to the schedule and paragraph 13 proposed by\nEnron. I look forward to discussing the terms of these documents at your\nearliest convenience. The form of the Letter of Credit and the guaranty are\nstill under review by Welch's and MWE."
    assert signature == mcdermott_email['signature']

def test_extract_signature():
    talon.init()
    body_without_signature, signature = SignatureExtraction().extract_signature(signature_email_1['text_clean'], signature_email_1['sender_email_address'])
    assert body_without_signature.strip() == "Gil:\nBefore any brokerage account can be opened, you need to advise the RAC group. Please send an email to the three persons (\"Cc\") above explaining what accounts Enron North America Corp. (\"ENA\") requires and the purpose of those accounts (if you have not already done so). ENA has a brokerage agreement with Bear Stearns Compamies, Inc. but not with Bear Stearns International Limited. ENA already has resolutions for opening brokerage accounts."
    assert signature == signature_email_1['signature']
