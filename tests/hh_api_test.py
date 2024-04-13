import json

import pytest

from fetch.hh_api import HhApi

BASE_URL = 'https://api.hh.ru/employers/'
FILE = 'data/test_fetch.json'
companies = {
    '1': '3302317',
    '2': '1234567890987654321',
}


@pytest.fixture
def fetcher():
    return HhApi(companies=companies, file=FILE, url=BASE_URL)


def test_init(fetcher):
    assert fetcher._HhApi__companies == {
        '1': '3302317',
        '2': '1234567890987654321',
    }
    assert fetcher._HhApi__file == 'data/test_fetch.json'
    assert fetcher._HhApi__base_url == 'https://api.hh.ru/employers/'


def test_fetch_data(fetcher):
    fetcher.fetch_data()
    with open(FILE) as f:
        data = json.load(f)
    assert len(data) == 1
