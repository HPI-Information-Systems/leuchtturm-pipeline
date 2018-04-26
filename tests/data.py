# flake8: noqa
"""Data to test the aggregation of correspondent information."""

# after correspondent data extraction
shackleton_emails = [
  {
    "signature": "Sara Shackleton\nEnron Wholesale Services\n1400 Smith Street, EB3801a\nHouston, TX 77002\nPh: (713) 853-5620\nFax: (713) 646-3490",
    "sender_email_address": "sara.shackleton@enron.com",
    "sender_name": "Sara Shackleton",
    "recipients": [
      {
        "name": "",
        "email": "susan.musch@enron.com"
      }
    ],
    "phone_numbers_office": [
      "(713) 853-5620"
    ],
    "phone_numbers_cell": [],
    "phone_numbers_fax": [
      "(713) 646-3490"
    ],
    "phone_numbers_home": [],
    "email_addresses_from_signature": [],
    "sender_aliases": [
      "Sara Shackleton"
    ],
    "writes_to": [
      "susan.musch@enron.com"
    ]
  },
  {
    "signature": "Sara\nEnron Wholesale Services\n1400 Smith Street, EB3801a\nHouston, TX 77002\nPh: (777) 777-7777\nFax: (713) 646-3490",
    "sender_email_address": "sara.shackleton@enron.com",
    "sender_name": "Sara Shackleton",
    "recipients": [
      {
        "name": "",
        "email": "m..melman@enron.com"
      },
      {
        "name": "",
        "email": "joseph.henry@enron.com"
      },
      {
        "name": "",
        "email": "donna.lowry@enron.com"
      },
      {
        "name": "",
        "email": "cassandra.schultz@enron.com"
      },
      {
        "name": "",
        "email": "tanya.rohauer@enron.com"
      },
      {
        "name": "",
        "email": "donna.lowry@enron.com"
      },
      {
        "name": "",
        "email": "cassandra.schultz@enron.com"
      },
      {
        "name": "",
        "email": "tanya.rohauer@enron.com"
      }
    ],
    "phone_numbers_office": [
      "(777) 777-7777"
    ],
    "phone_numbers_cell": [],
    "phone_numbers_fax": [
      "(713) 646-3490"
    ],
    "phone_numbers_home": [],
    "email_addresses_from_signature": [],
    "sender_aliases": [
      "Sara"
    ],
    "writes_to": [
      "m..melman@enron.com",
      "tanya.rohauer@enron.com",
      "cassandra.schultz@enron.com",
      "joseph.henry@enron.com",
      "donna.lowry@enron.com"
    ]
  }
]

aggregated_shackleton_correspondent_object = {
  "email_addresses": ["sara.shackleton@enron.com"],
  "identifying_names": ["Sara Shackleton"],
  "source_count": 2,
  "phone_numbers_office": [
    "(777) 777-7777",
    "(713) 853-5620"
  ],
  "phone_numbers_cell": [],
  "phone_numbers_fax": [
    "(713) 646-3490"
  ],
  "phone_numbers_home": [],
  "email_addresses_from_signature": [],
  "aliases": [],
  "aliases_from_signature": [
    "Sara Shackleton",
    "Sara"
  ],
  "writes_to": [
    "m..melman@enron.com",
    "tanya.rohauer@enron.com",
    "cassandra.schultz@enron.com",
    "susan.musch@enron.com",
    "joseph.henry@enron.com",
    "donna.lowry@enron.com"
  ],
  "signatures": [
    "Sara Shackleton\nEnron Wholesale Services\n1400 Smith Street, EB3801a\nHouston, TX 77002\nPh: (713) 853-5620\nFax: (713) 646-3490",
    "Sara\nEnron Wholesale Services\n1400 Smith Street, EB3801a\nHouston, TX 77002\nPh: (777) 777-7777\nFax: (713) 646-3490"
  ]
}
