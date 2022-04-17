"""
Digikala.com batch-mode crawler
Author: Saeid.S.Nobakht
"""

import logging
from config import CONFIG
from DigikalaCrawler import DigikalaCrawler


def main():
    logging.basicConfig(level=CONFIG['logging-level'])
    dgc = DigikalaCrawler(list_item_page_url=CONFIG['list-item-page-url'],
                          sort_by=CONFIG['sort-by']['newest'],
                          starting_page_number=CONFIG['starting-page-number'],
                          ending_page_number=CONFIG['ending-page-number'],
                          item_per_page=CONFIG['item-per-page'],
                          download_images=CONFIG['download-images-flag'],
                          image_download_chunk_size=CONFIG['image-download-chunk-size'],
                          download_images_only_if_not_exist=CONFIG['download-image-only-if-not-exist'],
                          user_agent=CONFIG['user-agent'],
                          results_path=CONFIG['results-path'],
                          retry_times=CONFIG['retry-times'],
                          backoff_factor=CONFIG['backoff-factor'],
                          default_timeout=CONFIG['default-timeout'],
                          all_data_file_name=CONFIG['all-data-file-name'],
                          url_prefix=CONFIG['url-prefix'],
                          product_detail_link_pattern=CONFIG['product-detail-link-pattern'])
    dgc.start()

    print("Done!")


if __name__ == '__main__':
    main()
