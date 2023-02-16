# regex patterns
import re
from typing import Optional


QUESTION = r"((^what)|(^why)|(^how)|(^can )|(^do )|(^does)|(^where)|(^who(se)? )|(who'?s )|(^which)|(^when)|(^is )|(^are )|(.*\?$))"
TRANSACTION = r"(.*((buy)|(cost)|(price)|(cheap)|(pricing)|(affordable)))"
INVESTIGATION = r"(.*((best)|(most)|(cheapest)|( vs)|( v\.s\.)))"
URL = r"https?://(www\.)?[\w\-_+]*(\.\w{2,4}){0,2}/"

PAGE_TYPE_REGEX_DICTS = {
    'standard':
        [
            {
                'page_type': 'category',
                'element': 'html',
                'regex': 'page[Tt]ype\\W{0,3}:\\W*(?:(?:category)|(?:collection))'
             },
            {
                'page_type': 'product',
                'element': 'html',
                'regex': 'page[Tt]ype\\W{0,3}?:\\W*(?:(?:product)|(?:pdp))'
             },
            {
                'page_type': 'blog',
                'element': 'html',
                'regex': 'page[Tt]ype\\W{0,3}?:\\W*blog'
            },
            {
                'page_type': 'blog',
                'element': 'url',
                'regex': '/blog/'
            },
        ],
    'shopify':
        [
            {
                'page_type': 'category',
                'element': 'html',
                'regex': 'page[Tt]ype\\W{0,3}:\\W*(?:(?:category)|(?:collection))'
             },
            {
                'page_type': 'product',
                'element': 'html',
                'regex': 'page[Tt]ype\\W{0,3}?:\\W*(?:(?:product)|(?:pdp))'
             },
            {
                'page_type': 'blog',
                'element': 'html',
                'regex': 'page[Tt]ype\\W{0,3}?:\\W*blog'
            },
        ]
}


url_regex = re.compile(URL)

url_strip_pattern = 'https?://(?:www.)?[\\w\\-\\+_]*(?:.\\w{2,4})?(?:.\\w{2,4})?(/[^\\?]*)'


def strip_url(url: str) -> str:
    url = url.split('?')[0]
    _match = url_regex.match(url)
    if _match:
        return url[_match.span()[1] - 1:]
    else:
        return url


def url_extract_parameter(url: str) -> Optional[str]:
    if '?' in url:
        return url.split('?')[1]
    else:
        return None


def url_strip_domain(url: str) -> Optional[str]:
    _match = url_regex.match(url)
    if _match:
        return url[_match.span()[1] - 1:]
    else:
        return url
