"""
Digikala.com batch-mode crawler
Author: Saeid.S.Nobakht
"""

import logging

CONFIG = {
    'logging-level': logging.ERROR,
    'conf1': 1,
    'user-agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
    'item-per-page': 36,
    'results-path': 'results',
    'all-data-file-name': 'all_data.csv',
    'url-prefix': 'https://www.digikala.com',
    'product-detail-link-pattern': 'https://www.digikala.com/product/dkp-%s/',
    'list-item-page-url': 'https://www.digikala.com/search/category-mobile-phone/',  # sample category link
    'image-download-chunk-size': 1024,
    'starting-page-number': 1,  # 53
    'ending-page-number': 53,  # 242
    'backoff-factor': 1,
    'retry-times': 3,
    'default-timeout': 30,
    'download-images-flag': True,
    'download-image-only-if-not-exist': True,
    'kill-flag': True,
    'sort-by': {
        'newest': 1,
        'most-viewed': 4,
        'most-sold': 7,
        'cheapest': 20,
        'most-expensive': 21,
        'most-popular': 22,
        'fastest-delivery': 25,
    }
}
