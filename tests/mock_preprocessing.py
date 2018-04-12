"""Provide unprocessed mock data for tests and also expected results."""


# email decoding
decode_raw_simple = r"""{"doc_id":"8371f964-bedd-48e0-8315-da88a2332688","path":"file:\/Users\/jh\/Desktop\/pipeline\/emails\/enron_weird.eml","raw":"Message-ID: <21305125.1075840880749.JavaMail.evans@thyme>\nDate: Fri, 4 Jan 2002 14:33:43 -0800 (PST)\nFrom: richard.a.stuckey@ssmb.com\nTo: .kimmel@enron.com, .flood@enron.com, .casaudoumecq@enron.com, \n\tlouise.kitchen@enron.com, .costa@enron.com\nSubject: RE: PA and ETA topics\nMime-Version: 1.0\nContent-Type: text\/plain; charset=us-ascii\nContent-Transfer-Encoding: 7bit\nX-From: \"Stuckey, Richard A [FI]\" <richard.a.stuckey@ssmb.com>@ENRON\nX-To: Kimmel, Debra [FI] <debra.kimmel@ssmb.com>, Flood, Scott L [GCO] <scott.l.flood@ssmb.com>, Casaudoumecq, John [FI] <john.casaudoumecq@ssmb.com>, Kitchen, Louise <\/O=ENRON\/OU=NA\/CN=RECIPIENTS\/CN=LKITCHEN>, Costa, Randall [GCO] <randall.costa@ssmb.com>\nX-cc: \nX-bcc: \nX-Folder: \\ExMerge - Kitchen, Louise\\'Americas\\Netco EOL\nX-Origin: KITCHEN-L\nX-FileName: louise kitchen 2-7-02.pst\n\nTo be clear, that is 10:30am New York time\n\n>  -----Original Message-----\n> From: \tKimmel, Debra [FI]\n> Sent:\tFriday, January 04, 2002 5:21 PM\n> To:\tFlood, Scott L [GCO]; Stuckey, Richard A [FI]; Casaudoumecq, John\n> [FI]; 'Kitchen, Louise'; Costa, Randall [GCO]\n> Subject:\tPA and ETA topics\n>\n>\n> A conference call has been scheduled for Monday, January 7th for 10:30am\n> to discuss the PA and ETA topics with Enron.\n> Below is the information for the conference call:\n>\n> Toll Free Number:\t888-780-9655\n> Passcode:\t\tETA\n> Leader:\t\t\tRichard Stuckey\n> Confirm:\t\t656118\n>\n>\n> Any questions, contact Debra Kimmel at 212-723-9472\n>\n> Thanks,\n> Debra","attachments":[]}"""
decode_raw_multipart = r"""{"raw":"Received: from dncedge1.dnc.org (192.168.185.10) by DNCHUBCAS1.dnc.org\r\n (192.168.185.12) with Microsoft SMTP Server (TLS) id 14.3.224.2; Wed, 27 Apr\r\n 2016 12:55:27 -0400\r\nReceived: from server555.appriver.com (8.19.118.102) by dncwebmail.dnc.org\r\n (192.168.10.221) with Microsoft SMTP Server id 14.3.224.2; Wed, 27 Apr 2016\r\n 12:55:20 -0400\r\nReceived: from [10.87.0.113] (HELO inbound.appriver.com)  by\r\n server555.appriver.com (CommuniGate Pro SMTP 6.0.4)  with ESMTP id 883154371\r\n for KaplanJ@dnc.org; Wed, 27 Apr 2016 11:55:28 -0500\r\nX-Note-AR-ScanTimeLocal: 4\/27\/2016 11:55:26 AM\r\nX-Policy: dnc.org\r\nX-Primary: kaplanj@dnc.org\r\nX-Note: This Email was scanned by AppRiver SecureTide\r\nX-Note: SecureTide Build: 4\/25\/2016 6:59:12 PM UTC\r\nX-Virus-Scan: V-\r\nX-Note: SPF: IP:209.85.161.175 DOM:gmail.com ADDR:evanschrum@gmail.com\r\nX-Note: SPF: Pass\r\nX-Note-SnifferID: 0\r\nX-Note: TCH-CT\/SI:0-280\/SG:5 4\/27\/2016 11:55:08 AM\r\nX-GBUdb-Analysis: 1, 192.168.1.134, Ugly c=0.479592 p=-0.542857 Source Normal\r\nX-Signature-Violations: 0-0-0-11670-c\r\nX-Note-419: 15.6183 ms. Fail:0 Chk:1324 of 1324 total\r\nX-Note: SCH-CT\/SI:0-1324\/SG:1 4\/27\/2016 11:55:08 AM\r\nX-Note: Spam Tests Failed: \r\nX-Country-Path: PRIVATE->->United States->\r\nX-Note-Sending-IP: 209.85.161.175\r\nX-Note-Reverse-DNS: mail-yw0-f175.google.com\r\nX-Note-Return-Path: evanschrum@gmail.com\r\nX-Note: User Rule Hits: \r\nX-Note: Global Rule Hits: G275 G276 G277 G278 G282 G283 G406 G667 \r\nX-Note: Encrypt Rule Hits: \r\nX-Note: Mail Class: VALID\r\nX-Note: Headers Injected\r\nReceived: from mail-yw0-f175.google.com ([209.85.161.175] verified)  by\r\n inbound.appriver.com (CommuniGate Pro SMTP 6.1.7)  with ESMTPS id 135427804\r\n for KaplanJ@dnc.org; Wed, 27 Apr 2016 11:55:26 -0500\r\nReceived: by mail-yw0-f175.google.com with SMTP id j74so82805087ywg.1\r\n        for <KaplanJ@dnc.org>; Wed, 27 Apr 2016 09:55:26 -0700 (PDT)\r\nDKIM-Signature: v=1; a=rsa-sha256; c=relaxed\/relaxed;\r\n        d=gmail.com; s=20120113;\r\n        h=from:content-transfer-encoding:mime-version:subject:message-id:date\r\n         :references:in-reply-to:to;\r\n        bh=ZQqckExGviBcMOCjIN+QMeRWXslpBPQtx4l0Iu\/nup4=;\r\n        b=dC0Sn0WJh6AQXG9trsXiAuqfXTre5JHXoZ6YsobHjEU3aoYUKbp\/vw2ReNCmz7wSCp\r\n         vGVkNFYXxybiyWwECxELFBgu82DgSDrrlSmx7rHd3A+Zuykm4FzIQ4n5YXVlkMcv2lOq\r\n         fkBgWKkHnKAH8cgSedunGBcGh4k6QNbXVCFh1+FLXQefWeg3DQ57W4mi\/2ThFyktE15a\r\n         JwAJqdcruVUsvZGEL0dpgQkJSCP2LdXUuKDoMrgJe3w8K2QZw6eb\/nUCFXcWeSXFYhmu\r\n         a0IEafQExeF3xL32PEIYyOBaMsAzhpDzLV6i3E5A1WyjhVVRNin2OUzeZNpO4bSc1TVk\r\n         agdw==\r\nX-Google-DKIM-Signature: v=1; a=rsa-sha256; c=relaxed\/relaxed;\r\n        d=1e100.net; s=20130820;\r\n        h=x-gm-message-state:from:content-transfer-encoding:mime-version\r\n         :subject:message-id:date:references:in-reply-to:to;\r\n        bh=ZQqckExGviBcMOCjIN+QMeRWXslpBPQtx4l0Iu\/nup4=;\r\n        b=iDKsMaltckIh4jlQApa4mUJs3xHmAG1iDZcKVmU6ElxpLG8YEGo1DhfaFE6r6uzK27\r\n         cZuaorfUcdKCfPaBDIk4qxoH6VkA6bRqQjcEUIu8AVSplDF1xI8YhH+CRcT3oekwv8uc\r\n         OiFJ5JRQtJ6ePSEZf8hkniaRYJKavTHRTeg94pF0CnpJFZODsKf1nFKlDqCb6M0ejbzz\r\n         3I9gkaE4VFAMUaNtNP0d16NEnBEG7Gx6aXfEU0xt3\/v8xWcho3hi65QYNkPOdHsPHC89\r\n         5zcKImM+RfGevBOdVeqOOOfu+cgf\/ZMQ80fYFbTs3l\/JQxbmmb6KFzr0Kk5lasBZkVXX\r\n         zaXA==\r\nX-Gm-Message-State: AOPr4FVBRli1AWJQhMyl4abRorl\/T\/XWxRaGbbPwmVOdioTx4YkDhVcARTqqHtgF74z6nQ==\r\nX-Received: by 10.13.227.69 with SMTP id m66mr5461837ywe.302.1461776125789;\r\n        Wed, 27 Apr 2016 09:55:25 -0700 (PDT)\r\nReturn-Path: <evanschrum@gmail.com>\r\nReceived: from [192.168.1.134] (62.sub-70-192-214.myvzw.com. [70.192.214.62])\r\n        by smtp.gmail.com with ESMTPSA id\r\n m8sm2388931ywb.18.2016.04.27.09.55.20        for <KaplanJ@dnc.org>\r\n        (version=TLSv1\/SSLv3 cipher=OTHER);        Wed, 27 Apr 2016 09:55:20\r\n -0700 (PDT)\r\nFrom: Michael Schrum <evanschrum@gmail.com>\r\nContent-Type: multipart\/alternative;\r\n\tboundary=\"Apple-Mail-78A0FDCF-6FDC-44C7-8B04-0C430609D0EC\"\r\nContent-Transfer-Encoding: 7bit\r\nSubject: Re: Schrum didn't return my call :(\r\nMessage-ID: <17A1DCE7-EA38-4A68-8352-A41D777E2943@gmail.com>\r\nDate: Wed, 27 Apr 2016 12:55:19 -0400\r\nReferences: <95AFEEF8AB22CE4E8CA3F8E6FBCB8CD10129F74D16E1@AUFC-S1.AUFC.local> <F58BE3EA-DD9E-47DB-B532-1DDD9C6E5A5F@dnc.org> <86E553A9-6137-49C9-8009-C272DAE5E0FF@gmail.com>\r\nIn-Reply-To: <86E553A9-6137-49C9-8009-C272DAE5E0FF@gmail.com>\r\nTo: \"Kaplan, Jordan\" <KaplanJ@dnc.org>\r\nX-Mailer: iPhone Mail (13C75)\r\nX-MS-Exchange-Organization-AVStamp-Mailbox: MSFTFF;1;0;0 0 0\r\nX-MS-Exchange-Organization-AuthSource: dncedge1.dnc.org\r\nX-MS-Exchange-Organization-AuthAs: Anonymous\r\nMIME-Version: 1.0\r\n\r\n--Apple-Mail-78A0FDCF-6FDC-44C7-8B04-0C430609D0EC\r\nContent-Type: text\/plain; charset=\"utf-8\"\r\nContent-Transfer-Encoding: quoted-printable\r\nX-WatchGuard-AntiVirus: part scanned. clean action=allow\r\n\r\nI called him=20\r\n\r\n> On Apr 27, 2016, at 12:24 PM, Michael Schrum <evanschrum@gmail.com> wrote:=\r\n\r\n>=20\r\n> I'll call him back but the boss won't call him=20\r\n>=20\r\n>> On Apr 27, 2016, at 11:40 AM, Kaplan, Jordan <KaplanJ@dnc.org> wrote:\r\n>>=20\r\n>> Don=E2=80=99t be that guy - you aren=E2=80=99t that guy. =20\r\n>>=20\r\n>> He just wants you to call Peter angelos.=20\r\n>>=20\r\n>>=20\r\n>>=20\r\n>> Jordan Kaplan\r\n>> National Finance Director\r\n>> Democratic National Committee\r\n>> (202) 488-5002 (o) | (312) 339-0224 (c)\r\n>> kaplanj@dnc.org\r\n>>=20\r\n>> <EFA0E494-461C-4D20-85BC-5D1CD9801DD6[11].png>\r\n>>=20\r\n>>> Begin forwarded message:\r\n>>>=20\r\n>>> From: Brad Woodhouse <woodhouse@americansunitedforchange.org>\r\n>>> Subject: Schrum didn't return my call :(\r\n>>> Date: April 27, 2016 at 11:43:05 AM EDT\r\n>>> To: \"kaplanj@dnc.org\" <kaplanj@dnc.org>\r\n>>>=20\r\n>>> =20\r\n>>> Brad Woodhouse\r\n>>> President\r\n>>> Americans United for Change\r\n>>> 202-552-0160 office\r\n>>> 202-251-5669 cell\r\n>>> woodhouse@americansunitedforchange.org\r\n>>=20\r\n\r\n--Apple-Mail-78A0FDCF-6FDC-44C7-8B04-0C430609D0EC\r\nContent-Type: text\/html; charset=\"utf-8\"\r\nContent-Transfer-Encoding: quoted-printable\r\nX-WatchGuard-AntiVirus: part scanned. clean action=allow\r\n\r\n<html><head>\r\n<meta http-equiv=3D\"Content-Type\" content=3D\"text\/html; charset=3Dutf-8\"><\/=\r\nhead><body dir=3D\"auto\"><div><\/div><div>I called him&nbsp;<\/div><div><br>On=\r\n Apr 27, 2016, at 12:24 PM, Michael Schrum &lt;<a href=3D\"mailto:evanschrum=\r\n@gmail.com\">evanschrum@gmail.com<\/a>&gt; wrote:<br><br><\/div><blockquote ty=\r\npe=3D\"cite\"><div><div><\/div><div>I'll call him back but the boss won't call=\r\n him&nbsp;<\/div><div><br>On Apr 27, 2016, at 11:40 AM, Kaplan, Jordan &lt;<=\r\na href=3D\"mailto:KaplanJ@dnc.org\">KaplanJ@dnc.org<\/a>&gt; wrote:<br><br><\/d=\r\niv><blockquote type=3D\"cite\"><div>\r\n\r\n\r\n\r\n\r\nDon=E2=80=99t be that guy - you aren=E2=80=99t that guy. &nbsp;\r\n<div class=3D\"\"><br class=3D\"\">\r\n<\/div>\r\n<div class=3D\"\">He just wants you to call Peter angelos.&nbsp;<br class=3D\"=\r\n\">\r\n<div class=3D\"\"><br class=3D\"\">\r\n<\/div>\r\n<div class=3D\"\"><br class=3D\"\">\r\n<div class=3D\"\"><br class=3D\"\">\r\nJordan Kaplan<br class=3D\"\">\r\nNational Finance Director<br class=3D\"\">\r\nDemocratic National Committee<br class=3D\"\">\r\n(202) 488-5002 (o) | (312) 339-0224 (c)<br class=3D\"\">\r\n<a href=3D\"mailto:kaplanj@dnc.org\" class=3D\"\">kaplanj@dnc.org<\/a><br class=\r\n=3D\"\">\r\n<span style=3D\"color: rgb(0, 0, 0); font-family: Helvetica;  font-style: no=\r\nrmal; font-variant: normal; font-weight: normal; letter-spacing: normal; li=\r\nne-height: normal; orphans: 2; text-align: -webkit-auto; text-indent: 0px; =\r\ntext-transform: none; white-space: normal; widows: 2; word-spacing: 0px; -w=\r\nebkit-text-size-adjust: auto; -webkit-text-stroke-width: 0px; \"><span><br c=\r\nlass=3D\"Apple-interchange-newline\" style=3D\"color: rgb(0, 0, 0); font-famil=\r\ny: Helvetica;  font-style: normal; font-variant: normal; font-weight: norma=\r\nl; letter-spacing: normal; line-height: normal; orphans: 2; text-align: -we=\r\nbkit-auto; text-indent: 0px; text-transform: none; white-space: normal; wid=\r\nows: 2; word-spacing: 0px; -webkit-text-size-adjust: auto; -webkit-text-str=\r\noke-width: 0px; \">\r\n<span style=3D\"color: rgb(0, 0, 0); font-family: Helvetica;  font-style: no=\r\nrmal; font-variant: normal; font-weight: normal; letter-spacing: normal; li=\r\nne-height: normal; orphans: 2; text-align: -webkit-auto; text-indent: 0px; =\r\ntext-transform: none; white-space: normal; widows: 2; word-spacing: 0px; -w=\r\nebkit-text-size-adjust: auto; -webkit-text-stroke-width: 0px; \"><span>&lt;E=\r\nFA0E494-461C-4D20-85BC-5D1CD9801DD6[11].png&gt;<\/span>\r\n<\/span><\/span><\/span><\/div>\r\n<div><br class=3D\"\">\r\n<blockquote type=3D\"cite\" class=3D\"\">\r\n<div class=3D\"\">Begin forwarded message:<\/div>\r\n<br class=3D\"Apple-interchange-newline\">\r\n<div style=3D\"margin-top: 0px; margin-right: 0px; margin-bottom: 0px; margi=\r\nn-left: 0px;\" class=3D\"\">\r\n<span style=3D\"font-family: -webkit-system-font, Helvetica Neue, Helvetica,=\r\n sans-serif; color:rgba(0, 0, 0, 1.0);\" class=3D\"\"><b class=3D\"\">From:\r\n<\/b><\/span><span style=3D\"font-family: -webkit-system-font, Helvetica Neue,=\r\n Helvetica, sans-serif;\" class=3D\"\">Brad Woodhouse &lt;<a href=3D\"mailto:wo=\r\nodhouse@americansunitedforchange.org\" class=3D\"\">woodhouse@americansunitedf=\r\norchange.org<\/a>&gt;<br class=3D\"\">\r\n<\/span><\/div>\r\n<div style=3D\"margin-top: 0px; margin-right: 0px; margin-bottom: 0px; margi=\r\nn-left: 0px;\" class=3D\"\">\r\n<span style=3D\"font-family: -webkit-system-font, Helvetica Neue, Helvetica,=\r\n sans-serif; color:rgba(0, 0, 0, 1.0);\" class=3D\"\"><b class=3D\"\">Subject:\r\n<\/b><\/span><span style=3D\"font-family: -webkit-system-font, Helvetica Neue,=\r\n Helvetica, sans-serif;\" class=3D\"\"><b class=3D\"\">Schrum didn't return my c=\r\nall :(<\/b><br class=3D\"\">\r\n<\/span><\/div>\r\n<div style=3D\"margin-top: 0px; margin-right: 0px; margin-bottom: 0px; margi=\r\nn-left: 0px;\" class=3D\"\">\r\n<span style=3D\"font-family: -webkit-system-font, Helvetica Neue, Helvetica,=\r\n sans-serif; color:rgba(0, 0, 0, 1.0);\" class=3D\"\"><b class=3D\"\">Date:\r\n<\/b><\/span><span style=3D\"font-family: -webkit-system-font, Helvetica Neue,=\r\n Helvetica, sans-serif;\" class=3D\"\">April 27, 2016 at 11:43:05 AM EDT<br cl=\r\nass=3D\"\">\r\n<\/span><\/div>\r\n<div style=3D\"margin-top: 0px; margin-right: 0px; margin-bottom: 0px; margi=\r\nn-left: 0px;\" class=3D\"\">\r\n<span style=3D\"font-family: -webkit-system-font, Helvetica Neue, Helvetica,=\r\n sans-serif; color:rgba(0, 0, 0, 1.0);\" class=3D\"\"><b class=3D\"\">To:\r\n<\/b><\/span><span style=3D\"font-family: -webkit-system-font, Helvetica Neue,=\r\n Helvetica, sans-serif;\" class=3D\"\">&quot;<a href=3D\"mailto:kaplanj@dnc.org=\r\n\" class=3D\"\">kaplanj@dnc.org<\/a>&quot; &lt;<a href=3D\"mailto:kaplanj@dnc.or=\r\ng\" class=3D\"\">kaplanj@dnc.org<\/a>&gt;<br class=3D\"\">\r\n<\/span><\/div>\r\n<br class=3D\"\">\r\n<div class=3D\"\">\r\n<div style=3D\"font-style: normal; font-variant: normal; font-weight: normal=\r\n; letter-spacing: normal; orphans: auto; text-align: start; text-indent: 0p=\r\nx; text-transform: none; white-space: normal; widows: auto; word-spacing: 0=\r\npx; -webkit-text-stroke-width: 0px; font-size: x-small; font-family: Tahoma=\r\n; direction: ltr;\" class=3D\"\">\r\n<div class=3D\"\"><\/div>\r\n<div dir=3D\"ltr\" class=3D\"\"><font size=3D\"2\" face=3D\"Tahoma\" class=3D\"\"><\/f=\r\nont>&nbsp;<\/div>\r\n<div class=3D\"\"><font size=3D\"2\" face=3D\"Tahoma\" class=3D\"\">Brad Woodhouse<=\r\n\/font><\/div>\r\n<div class=3D\"\"><font size=3D\"2\" face=3D\"tahoma\" class=3D\"\">President<\/font=\r\n><\/div>\r\n<div class=3D\"\"><font size=3D\"2\" face=3D\"tahoma\" class=3D\"\">Americans Unite=\r\nd for Change<\/font><\/div>\r\n<div class=3D\"\"><font size=3D\"2\" face=3D\"tahoma\" class=3D\"\">202-552-0160 of=\r\nfice<\/font><\/div>\r\n<div class=3D\"\"><font size=3D\"2\" face=3D\"tahoma\" class=3D\"\">202-251-5669 ce=\r\nll<\/font><\/div>\r\n<div class=3D\"\"><font size=3D\"2\" face=3D\"tahoma\" class=3D\"\"><a href=3D\"mail=\r\nto:woodhouse@americansunitedforchange.org\" class=3D\"\">woodhouse@americansun=\r\nitedforchange.org<\/a><\/font><\/div>\r\n<\/div>\r\n<\/div>\r\n<\/blockquote>\r\n<\/div>\r\n<br class=3D\"\">\r\n<\/div>\r\n<\/div>\r\n\r\n\r\n<\/div><\/blockquote><\/div><\/blockquote><\/body><\/html>=\r\n\r\n--Apple-Mail-78A0FDCF-6FDC-44C7-8B04-0C430609D0EC--\r\n"}"""


# email splitting
splitting_raw_org_msg = r"""{"doc_id":"1","raw":"Message-ID: <21305125.1075840880749.JavaMail.evans@thyme> \nDate: Fri, 4 Jan 2002 14:33:43 -0800 (PST)\nFrom: richard.a.stuckey@ssmb.com\nTo: .kimmel@enron.com, .flood@enron.com, .casaudoumecq@enron.com, \n\tlouise.kitchen@enron.com, .costa@enron.com\nSubject: RE: PA and ETA topics\nMime-Version: 1.0\nContent-Type: text/plain; charset=us-ascii\nContent-Transfer-Encoding: 7bit\nX-From: \"Stuckey, Richard A [FI]\" <richard.a.stuckey@ssmb.com>@ENRON\nX-To: Kimmel, Debra [FI] <debra.kimmel@ssmb.com>, Flood, Scott L [GCO] <scott.l.flood@ssmb.com>, Casaudoumecq, John [FI] <john.casaudoumecq@ssmb.com>, Kitchen, Louise </O=ENRON/OU=NA/CN=RECIPIENTS/CN=LKITCHEN>, Costa, Randall [GCO] <randall.costa@ssmb.com>\nX-cc: \nX-bcc: \nX-Folder: \\ExMerge - Kitchen, Louise\\'Americas\\Netco EOL\nX-Origin: KITCHEN-L\nX-FileName: louise kitchen 2-7-02.pst\n\nTo be clear, that is 10:30am New York time\n\n>  -----Original Message-----\n> From: \tKimmel, Debra [FI]\n> Sent:\tFriday, January 04, 2002 5:21 PM\n> To:\tFlood, Scott L [GCO]; Stuckey, Richard A [FI]; Casaudoumecq, John\n> [FI]; 'Kitchen, Louise'; Costa, Randall [GCO]\n> Subject:\tPA and ETA topics\n>\n>\n> A conference call has been scheduled for Monday, January 7th for 10:30am\n> to discuss the PA and ETA topics with Enron.\n> Below is the information for the conference call:\n>\n> Toll Free Number:\t888-780-9655\n> Passcode:\t\tETA\n> Leader:\t\t\tRichard Stuckey\n> Confirm:\t\t656118\n>\n>\n> Any questions, contact Debra Kimmel at 212-723-9472\n>\n> Thanks,\n> Debra","attachments":[]}"""
splitting_raw_fwd = r"""{"doc_id":"2","raw":"Date: Wed, 15 Nov 2000 01:32:00 -0800\nFrom: brian.hoskins@enron.com\nTo: eric.bass@enron.com, hector.campos@enron.com\nSubject: Another slam on Gore!\nX-From: Brian Hoskins\nX-To: Eric Bass, Hector Campos\nX-cc: \nX-bcc: \n\n\n\n\n\n\n\nBrian T. Hoskins\nEnron Broadband Services\n713-853-0380 (office)\n713-412-3667 (mobile)\n713-646-5745 (fax)\nBrian_Hoskins@enron.net\n\n\n----- Forwarded by Brian Hoskins/Enron Communications on 11/15/00 09:40 AM \n-----\n\n\tKori Loibl@ECT\n\t11/14/00 04:03 PM\n\t\t \n\t\t To: Alicia Perkins/HOU/EES@EES, Purvi Patel/HOU/ECT@ECT, Beau \nRatliff/HOU/EES@EES, Lucy Ortiz/HOU/ECT@ECT, Scott Pleus/Enron \nCommunications@Enron Communications, Don Baughman/HOU/ECT@ECT, Brian \nHoskins/Enron Communications@Enron Communications, Tobin Carlson/HOU/ECT@ECT\n\t\t cc: \n\t\t Subject: Another slam on Gore!\n\nThis is great!\n\n\n\n"}"""
splitting_raw_dnc = r"""{"doc_id":"3","raw":"From: \"Lopez, Jacquelyn K.  (Perkins Coie)\" <JacquelynLopez@perkinscoie.com>\nTo: \"Freundlich, Christina\" <FreundlichC@dnc.org>, \"Lykins, Tyler\" <LykinsT@dnc.org>, \"Reif, Eric\" <ReifE@dnc.org>, EMail-Vetting_D <EMail-Vetting_D@dnc.org>\nSubject: RE: For approval: Factivists for this weekend\nDate: Fri, 13 May 2016 21:27:16 +0000\n\n\n\n\nDo we have the rights to the music in the video re: how the GOP created trump?\n\n\n\nJacquelyn Lopez | Perkins Coie LLP\n\nASSOCIATE*\n\n700 Thirteenth Street, N.W. Suite 600\n\nWashington, DC 20005-3960\n\nD. +1.202.654.6371\n\nF. +1.202.654.9949\n\nE. JacquelynLopez@perkinscoie.com\n\n*Admitted in State of Florida; Admission to DC Bar pending. \n\n\n\nFrom: Freundlich, Christina [mailto:FreundlichC@dnc.org]  \nSent: Friday, May 13, 2016 2:20 PM  \nTo: Lykins, Tyler; Reif, Eric; EMail-Vetting_D  \nSubject: RE: For approval: Factivists for this weekend\n\n\n\nGood\n\n\n\n  \n  \n\nOn Fri, May 13, 2016 at 11:12 AM -0700, \"Lykins, Tyler\" <LykinsT@dnc.org>\nwrote:\n\nGreat\n\n\n\nFrom: Reif, Eric  \nSent: Friday, May 13, 2016 1:52 PM  \nTo: EMail-Vetting_D  \nSubject: For approval: Factivists for this weekend\n\n\n\nHI all -- Well send this to Factivists tomorrow. Thanks!\n\n\n\n\n\nSender: 2016 DNC Factivists\n\nSubject: The Breakfast Club\n\n\n\n  \nHey there, NAME --\n\nRemember that classic 80s movie where high school kids from all different\nwalks of life come together over weekend detention and realize theyre not so\ndifferent after all? Well, Speaker Paul Ryan and the Republican Partys\npresumptive nominee for president, Donald Trump, spent some time together on\nThursday morning and realized there is plenty they have in common.\n\n  \nIn fact, they issued a statement (just like The Breakfast Clubs assignment!)\nsaying how crucial it was that \"Republicans unite around our shared\nprinciples, advance a conservative agenda, and do all we can to win this\nfall,\" and \"while we recognize our few differences, we recognize that there\nare also many important areas of common ground\" and ultimately \"remain\nconfident theres a great opportunity to unify our party and win this fall.\"  \nYes, the Republican Party is unifying around a candidate who has stoked hatred\nand intolerance so much, some of his biggest fans are white nationalists:\n\nhttps://twitter.com/TheDemocrats/status/730173087736463360\n\n\n\nWhose approval rating among women is historically low -- with good reason:\n\n\n\nhttps://twitter.com/DNCWomen/status/730817363583602688\n\n\n\nAnd who is at odds with our most fundamental American values:\n\n\n\nhttps://twitter.com/TheDemocrats/status/730858701498793985\n\n\n\nBut really, it shouldnt surprise Speaker Ryan -- or anyone else, for that\nmatter -- that he should be able to find common ground with Donald Trump.\nAfter all, Trumps candidacy has given the Republican Party what theyve been\nasking for after years of sowing seeds of fear, hatred, greed, and\nintolerance:\n\nhttps://twitter.com/TheDemocrats/status/730769449146421249\n\n\n\nThe stakes have never been higher for electing Democrats this November, NAME.\nEspecially when the Republican nominee is as resistant to facts and fact-\nchecking as Trump is, your job is more important than ever to get the word out\nto your family, friends, and neighbors on what a disaster Trump -- and the\nrest of his Republican Party -- would be for our country, so tweet, share, and\nforward this email to everyone you can.\n\n\n\nThanks for your help, and talk soon!\n\n\n\n2016 DNC Factivists\n\n  \n\n* * *\n\n  \nNOTICE: This communication may contain privileged or other confidential\ninformation. If you have received it in error, please advise the sender by\nreply email and immediately delete the message and any attachments without\ncopying or disclosing the contents. Thank you.  \n\n","attachments":[]}"""


# header parsing
header_raw_indent = r"""{"header":">  -----Original Message-----\n> From: \tKimmel, Debra [FI]\n> Sent:\tFriday, January 04, 2002 5:21 PM\n> To:\tFlood, Scott L [GCO]; Stuckey, Richard A [FI]; Casaudoumecq, John\n> [FI]; 'Kitchen, Louise'; Costa, Randall [GCO]\n> Subject:\tPA and ETA topics\n"}"""
header_parsed_indent = r"""{"header":{"sender":{"name":"Debra Kimmel","email":""},"recipients":[{"name":"Scott L Flood","email":"","type":"to"},{"name":"Richard A Stuckey","email":"","type":"to"},{"name":"John Casaudoumecq","email":"","type":"to"},{"name":"Louise Kitchen","email":"","type":"to"},{"name":"Randall Costa","email":"","type":"to"}],"date":"2002-01-04T17:21:00Z","date_changed":false,"subject":"PA and ETA topics"}}"""

header_raw_fwd = r"""{"header": "---------------------- Forwarded by David Oxley/HOU/ECT on 12/04/2000 08:28 \nAM ---------------------------\n\n\nKevin Hannon@ENRON COMMUNICATIONS on 12/02/2000 10:12:58 AM\nTo: Gina Corteselli/Corp/Enron@ENRON\ncc: Cindy Olson/Corp/Enron@ENRON, David Oxley/HOU/ECT@ECT@ENRON, Steven J \nKean/NA/Enron@Enron \nSubject: Re:"}"""
header_parsed_fwd = r"""{"header":{"sender":{"name":"Kevin Hannon","email":""},"recipients":[{"name":"Gina Corteselli","email":"","type":"to"},{"name":"Cindy Olson","email":"","type":"cc"},{"name":"David Oxley","email":"","type":"cc"},{"name":"Steven J Kean","email":"","type":"cc"}],"date":"2000-12-02T10:12:58Z","date_changed":false,"subject":"Re:"}}"""

header_raw_regular = r"""{"header": "Date: Fri, 15 Feb 2002 09:07:17 -0800\nFrom: susan.bailey@enron.com\nTo: sara.shackleton@enron.com\nSubject: RE: Thiele Kaolin Company\nX-From: Bailey, Susan </O=ENRON/OU=NA/CN=RECIPIENTS/CN=SBAILE2>\nX-To: Shackleton, Sara </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Sshackl>\nX-cc: \nX-bcc: "}"""
header_parsed_regular = r"""{"header":{"sender":{"name":"Susan Bailey","email":"susan.bailey@enron.com"},"recipients":[{"name":"Sara Shackleton","email":"sara.shackleton@enron.com","type":"to"}],"date":"2002-02-15T09:07:17Z","date_changed":false,"subject":"RE: Thiele Kaolin Company"}}"""

header_raw_dnc = r"""{"header":"Date: Wed, 04 May 2016 03:13:06 -0400\nTo: comers@dnc.org, kaplanj@dnc.org, rauscherr@dnc.org\nFrom: Contribution <postmaster@my.democrats.org>\nSubject: Contribution: Donate to help Democrats (new abroad) \/ Janet Weiler \/ 5.00 USD\n"}"""
header_parsed_dnc = r"""{"header":{"sender":{"name":"Contribution","email":"postmaster@my.democrats.org"},"recipients":[{"name":"","email":"comers@dnc.org","type":"to"},{"name":"","email":"kaplanj@dnc.org","type":"to"},{"name":"","email":"rauscherr@dnc.org","type":"to"}],"date":"2016-05-04T03:13:06Z","date_changed":false,"subject":"Contribution: Donate to help Democrats (new abroad) \/ Janet Weiler \/ 5.00 USD"}}"""

header_raw_deformed = r"""{"header":"Mark Elliott\n04\/23\/99 05:49 AM\nTo: Martin Rosell\/OSL\/ECT@ECT\ncc: Mark - ECT Legal Taylor\/HOU\/ECT@ECT \nSubject: Re: OMnet, etc.  \n"}"""
header_parsed_deformed = r"""{"header":{"sender":{"name":"Mark Elliott","email":""},"recipients":[{"name":"Martin Rosell","email":"","type":"to"},{"name":"Mark - Ect Legal Taylor","email":"","type":"cc"}],"date":"1999-04-23T05:49:00Z","date_changed":false,"subject":"Re: OMnet, etc."}}"""

header_raw_low_date = r"""{"header": "Date: Fri, 15 Feb 1991 09:07:17 -0800\nFrom: susan.bailey@enron.com\nTo: sara.shackleton@enron.com\nSubject: RE: Thiele Kaolin Company\nX-From: Bailey, Susan </O=ENRON/OU=NA/CN=RECIPIENTS/CN=SBAILE2>\nX-To: Shackleton, Sara </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Sshackl>\nX-cc: \nX-bcc: "}"""
header_parsed_low_date = r"""{"header":{"sender":{"name":"Susan Bailey","email":"susan.bailey@enron.com"},"recipients":[{"name":"Sara Shackleton","email":"sara.shackleton@enron.com","type":"to"}],"date":"1997-12-01T00:00:00Z","date_changed":true,"subject":"RE: Thiele Kaolin Company"}}"""

header_raw_high_date = r"""{"header": "Date: Fri, 15 Feb 2041 09:07:17 -0800\nFrom: susan.bailey@enron.com\nTo: sara.shackleton@enron.com\nSubject: RE: Thiele Kaolin Company\nX-From: Bailey, Susan </O=ENRON/OU=NA/CN=RECIPIENTS/CN=SBAILE2>\nX-To: Shackleton, Sara </O=ENRON/OU=NA/CN=RECIPIENTS/CN=Sshackl>\nX-cc: \nX-bcc: "}"""
header_parsed_high_date = r"""{"header":{"sender":{"name":"Susan Bailey","email":"susan.bailey@enron.com"},"recipients":[{"name":"Sara Shackleton","email":"sara.shackleton@enron.com","type":"to"}],"date":"2002-12-31T23:59:59Z","date_changed":true,"subject":"RE: Thiele Kaolin Company"}}"""

# language detection
lang_en_raw = r"""{"body":"This is a text written in one of the most spoken languages and it contains a speling error."}"""
lang_de_raw = r"""{"body":"Das ist ein Text in einer nicht ganz so wichtigen Sprache, aber er enthält auch Feler."}"""


# text cleaning
clean_raw = r"""{"body":"This is a text with \n\n\n much \t\rweird white\nspace. And look a unicode char: 🎨.. Also, it conatains a email: jas@enron.com, a link: https://enron.com and a phone number: 00493311234567!"}"""
