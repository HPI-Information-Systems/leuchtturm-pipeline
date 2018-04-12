"""Test preprocessing module."""

import ujson as json

from src.preprocessing import EmailDecoding, HeaderParsing, TextCleaning, LanguageDetection, EmailSplitting
from .mock_preprocessing import decode_raw_simple, decode_raw_multipart  # decoding
from .mock_preprocessing import splitting_raw_org_msg, splitting_raw_fwd, splitting_raw_dnc  # splitting
from .mock_preprocessing import (header_raw_indent, header_parsed_indent, header_raw_fwd, header_parsed_fwd,
                                 header_raw_regular, header_parsed_regular, header_raw_dnc, header_parsed_dnc,
                                 header_raw_deformed, header_parsed_deformed, header_raw_low_date, header_parsed_low_date,
                                 header_raw_high_date, header_parsed_high_date)  # header parsing
from .mock_preprocessing import lang_en_raw, lang_de_raw  # lang detection
from .mock_preprocessing import clean_raw
from src.util import get_config

config_enron = get_config('enron')
config_dnc = get_config('dnc')


def test_email_decoding_simple():
    """Simple emails should be decoded correctly."""
    tool = EmailDecoding()
    decoded = tool.run_on_document(decode_raw_simple)
    assert len(decode_raw_simple) >= len(decoded)
    assert 'Any questions, contact Debra Kimmel at 212-723-9472' in decoded


def test_email_decoding_mime():
    """Multipart emails should be decoded correctly."""
    tool = EmailDecoding()
    decoded = tool.run_on_document(decode_raw_multipart)
    assert len(decode_raw_multipart) > len(decoded)
    assert 'fkBgWKkHnKAH8cgSedunGBcGh4k6QNbXVCFh1+FLXQefWeg' not in decoded
    assert 'I\'ll call him back but the boss won\'t call him' in decoded


def test_email_splitting_on_org_msg():
    """Emails should be splitted into their parts. Standard inline headers should be recognised."""
    tool = EmailSplitting()
    assert len(tool.run_on_document(splitting_raw_org_msg)) == 2


def test_email_splitting_on_fwd():
    """Emails should be splitted into their parts. Fwd headers should be recognised."""
    tool = EmailSplitting()
    assert len(tool.run_on_document(splitting_raw_fwd)) == 2


def test_email_splitting_on_dnc():
    """Emails should be splitted into their parts. Dnc headers should be recognised."""
    tool = EmailSplitting()
    assert len(tool.run_on_document(splitting_raw_dnc)) == 3


def test_header_parsing_on_indented():
    """Headers should be parsed correctly for visually indented headers (>>)."""
    tool = HeaderParsing(config_enron, clean_subject=False, use_unix_time=False)
    assert tool.run_on_document(header_raw_indent) == header_parsed_indent


def test_header_parsing_on_fwd():
    """Headers should be parsed correctly for inline fwds."""
    tool = HeaderParsing(config_enron, clean_subject=False, use_unix_time=False)
    assert tool.run_on_document(header_raw_fwd) == header_parsed_fwd


def test_header_parsing_on_regular():
    """Standard headers should be parsed correctly."""
    tool = HeaderParsing(config_enron, clean_subject=False, use_unix_time=False)
    assert tool.run_on_document(header_raw_regular) == header_parsed_regular


def test_header_parsing_on_dnc():
    """Dnc headers should be parsed correctly."""
    tool = HeaderParsing(config_dnc, clean_subject=False, use_unix_time=False)
    assert tool.run_on_document(header_raw_dnc) == header_parsed_dnc


def test_header_parsing_on_deformed():
    """Deformed headers should be parsed correctly (e.g. no from field, instead startig with name)."""
    tool = HeaderParsing(config_enron, clean_subject=False, use_unix_time=False)
    assert tool.run_on_document(header_raw_deformed) == header_parsed_deformed

def test_header_parsing_low_date():
    """Date lower than specified should be parsed to minimal date."""
    tool = HeaderParsing(config_enron, clean_subject=False, use_unix_time=False)
    assert tool.run_on_document(header_raw_low_date) == header_parsed_low_date

def test_header_parsing_high_date():
    """Date greater than specified should be parsed to maximal date."""
    tool = HeaderParsing(config_enron, clean_subject=False, use_unix_time=False)
    assert tool.run_on_document(header_raw_high_date) == header_parsed_high_date

def test_language_detection():
    """Language should be detected correctly on at least English and German texts."""
    tool = LanguageDetection(read_from='body')
    assert json.loads(tool.run_on_document(lang_en_raw))['lang'] == 'en'
    assert json.loads(tool.run_on_document(lang_de_raw))['lang'] == 'de'


def test_text_cleaning():
    """Text cleaning should normalize whitespace and replace unicode chars."""
    tool = TextCleaning(read_from='body', write_to='body')
    cleaned = tool.run_on_document(clean_raw)
    assert '🎨' not in cleaned
    assert '\n\n' not in cleaned


def test_text_cleaning_strict():
    """Text cleaning strict should additionally remove emails, urls, telephone numbers, ..."""
    tool = TextCleaning(read_from='body', write_to='body', readable=False)
    cleaned = tool.run_on_document(clean_raw)
    assert '@enron.com' not in cleaned
    assert 'https://' not in cleaned
    assert '0049331' not in cleaned
