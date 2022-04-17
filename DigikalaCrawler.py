"""
Digikala.com batch-mode crawler
Author: Saeid.S.Nobakht
"""

from config import CONFIG
from TimeoutHTTPAdapter import TimeoutHTTPAdapter
import requests
from urllib3.util.retry import Retry
import os
import copy
import re
import json
from pathlib import Path
import pandas as pd


class DigikalaCrawler:
    def __init__(self, list_item_page_url, sort_by, starting_page_number, ending_page_number,
                 item_per_page, download_images, download_images_only_if_not_exist, user_agent,
                 results_path, retry_times, backoff_factor, default_timeout, all_data_file_name,
                 url_prefix, product_detail_link_pattern, image_download_chunk_size):
        self.all_data_file_abs_path = ""
        self.results_path = results_path
        self.out_data_format = "csv"  # currently only supports '.csv'
        self.list_item_page_url = list_item_page_url
        self.sort_by = sort_by
        self.item_per_page = item_per_page
        self.page_num = starting_page_number
        self.max_page_num = ending_page_number
        self.download_images_flag = download_images
        self.download_images_only_if_not_exist = download_images_only_if_not_exist
        self.user_agent = user_agent
        self.retry_times = retry_times
        self.backoff_factor = backoff_factor
        self.default_timeout = default_timeout
        self.all_data_file_name = all_data_file_name
        self.url_prefix = url_prefix
        self.product_detail_link_pattern = product_detail_link_pattern
        self.image_download_chunk_size = image_download_chunk_size

        # for describing the arguments, check this link
        # https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
        # the formula:  {backoff factor} * (2 ** ({number of total retries} - 1))
        self.retry_strategy = Retry(
            total=self.retry_times,
            backoff_factor=self.backoff_factor,
            method_whitelist=["HEAD", "GET", "OPTIONS"],
            status_forcelist=[429, 500, 502, 503, 504]
        )

        self.http = requests.Session()
        # we could override the default timeout in adapter level, or per request level
        adapter = TimeoutHTTPAdapter(timeout=self.default_timeout, max_retries=self.retry_strategy)
        self.http.mount('http://', adapter)
        self.http.mount('https://', adapter)
        self.http.headers.update({
            'User-Agent': self.user_agent,
        })

        self.columns = [
            'product-id',
            'title',
            'price',
            'category',
            'variant',
            'brand',
            'link',
            'seller-link',
            'seller-name',
            'guarantee',
            'quantity',
            'position',
            'first-image-link',
            'specs',
            'image-links']

        self.all_data = pd.DataFrame(columns=self.columns)

    def load_data(self):
        base_path = os.path.dirname(__file__)
        self.all_data_file_abs_path = os.path.join(base_path, self.results_path, self.all_data_file_name)
        if Path(self.all_data_file_abs_path).exists():
            self.all_data = pd.read_csv(self.all_data_file_abs_path, encoding='utf-8', dtype=object)
        else:
            self.all_data = pd.DataFrame(columns=self.columns)

    def extract_basic_data(self, item):
        data_row = {'product-id': item[2], 'link': self.url_prefix + eval(item[9])['product_url']}
        return data_row

    def extract_full_data(self, item, with_price_matched_items):
        null_replaced_string_6 = item[6].replace('null', '""')
        null_replaced_string_9 = item[9].replace('null', '""')
        data_row = {'product-id': item[2],
                    'link': self.url_prefix + eval(null_replaced_string_9)['product_url'],
                    'price': str(eval(null_replaced_string_6)['price']),
                    'title': item[4].strip(),
                    'category': eval(null_replaced_string_6)['category'],
                    'brand': eval(null_replaced_string_6)['brand'],
                    'variant': str(eval(null_replaced_string_6)['variant']),
                    'quantity': str(eval(null_replaced_string_6)['quantity']),
                    'position': str(eval(null_replaced_string_9)['position']),
                    'guarantee': '',
                    'seller-name': '',
                    'seller-link': '',
                    'first-image-link': item[11].split('?')[0],
                    'image-links': ''}

        df = pd.DataFrame(with_price_matched_items, columns=['seller-link', 'seller-name', 'product-id',
                                                             'seller-name-2', 'guarantee'], dtype=object)

        if (df['product-id'] == data_row['product-id']).any():
            sample = df.loc[df['product-id'] == data_row['product-id']].to_dict('records')[0]
            data_row['guarantee'] = sample['guarantee'].strip()
            data_row['seller-name'] = sample['seller-name'].strip()
            data_row['seller-link'] = sample['seller-link']

        return data_row

    def drop_columns(self, columns_name_list):
        self.all_data = self.all_data.drop(columns_name_list, axis=1)

    def download_images(self, data_row, item_page_content, download_path):
        images_link_pattern_1 = re.compile(
            r'(https://dkstatics-public.*?\.digikala\.com/digikala-products/(.*?)_(.*?)\.jpg).*?',
            flags=re.DOTALL
        )
        images_link_pattern_2 = re.compile(
            r'(https://dkstatics-public.*?\.digikala\.com/digikala-products/([0-9]+)\.jpg).*?',
            flags=re.DOTALL
        )
        images_link_pattern_3 = re.compile(
            r'(https://dkstatics-public.*?\.digikala\.com/digikala-products/([a-z0-9]+)\.jpg).*?',
            flags=re.DOTALL
        )

        if not data_row['image-links']:
            # extract images list
            images_pattern = re.compile(
                r'<script\stype=\"application/ld\+json\">(.*?\"image\".*?)</script>',
                flags=re.DOTALL)
            matches = images_pattern.findall(item_page_content)

            if not matches:
                print("No image found for this product")
                if CONFIG['kill-flag']:
                    exit(0)

            info_json = json.loads(matches[0].strip())

            images_links_list = []
            for product_image_link in info_json['image']:
                matched_image_url = images_link_pattern_1.findall(product_image_link)
                if not matched_image_url:
                    matched_image_url = images_link_pattern_2.findall(product_image_link)
                    if not matched_image_url:
                        matched_image_url = images_link_pattern_3.findall(product_image_link)
                        if not matched_image_url:
                            if CONFIG['kill-flag']:
                                print("Image in new format! ", product_image_link)
                                exit(0)
                        else:
                            image_file_abs_path = os.path.join(download_path, '%s_%s.jpg' % (
                                str(data_row['product-id']), matched_image_url[0][1]))
                    else:
                        image_file_abs_path = os.path.join(download_path, '%s_%s.jpg' % (
                            str(data_row['product-id']), matched_image_url[0][1]))
                else:
                    image_file_abs_path = os.path.join(download_path, '%s_%s.jpg' % (
                        str(data_row['product-id']), matched_image_url[0][2]))
                clean_image_url = matched_image_url[0][0]
                images_links_list.append(copy.deepcopy(clean_image_url))
                if self.download_images_flag:
                    if self.download_images_only_if_not_exist:
                        if not Path(image_file_abs_path).exists():
                            image_response = self.http.get(clean_image_url, stream=True)
                            if image_response.status_code == 200:
                                with open(image_file_abs_path, 'wb') as f:
                                    for chunk in image_response.iter_content(
                                            self.image_download_chunk_size):
                                        f.write(chunk)
                            print(">> image=%s saved!" % clean_image_url)
                        else:
                            print(">> image=%s already saved!" % clean_image_url)
                    else:
                        image_response = self.http.get(clean_image_url, stream=True)
                        if image_response.status_code == 200:
                            with open(image_file_abs_path, 'wb') as f:
                                for chunk in image_response.iter_content(self.image_download_chunk_size):
                                    f.write(chunk)
                else:
                    print("Skip downloading images.")

            data_row['image-links'] = json.dumps(images_links_list, ensure_ascii=False)
        else:
            images_links_list = json.loads(data_row['image-links'])

            for product_image_link in images_links_list:
                matched_image_url = images_link_pattern_1.findall(product_image_link)
                if not matched_image_url:
                    matched_image_url = images_link_pattern_2.findall(product_image_link)
                    if not matched_image_url:
                        matched_image_url = images_link_pattern_3.findall(product_image_link)
                        if not matched_image_url:
                            if CONFIG['kill-flag']:
                                print("Image in new format! ", product_image_link)
                                exit(0)
                        else:
                            image_file_abs_path = os.path.join(download_path, '%s_%s.jpg' % (
                                str(data_row['product-id']), matched_image_url[0][1]))
                    else:
                        image_file_abs_path = os.path.join(download_path, '%s_%s.jpg' % (
                            str(data_row['product-id']), matched_image_url[0][1]))
                else:
                    image_file_abs_path = os.path.join(download_path, '%s_%s.jpg' % (
                        str(data_row['product-id']), matched_image_url[0][2]))
                clean_image_url = matched_image_url[0][0]

                if self.download_images_flag:
                    if self.download_images_only_if_not_exist:
                        if not Path(image_file_abs_path).exists():
                            image_response = self.http.get(clean_image_url, stream=True)
                            if image_response.status_code == 200:
                                with open(image_file_abs_path, 'wb') as f:
                                    for chunk in image_response.iter_content(
                                            self.image_download_chunk_size):
                                        f.write(chunk)
                            print(">> image=%s saved!" % clean_image_url)
                        else:
                            print(">> image=%s already saved!" % clean_image_url)
                    else:
                        image_response = self.http.get(clean_image_url, stream=True)
                        if image_response.status_code == 200:
                            with open(image_file_abs_path, 'wb') as f:
                                for chunk in image_response.iter_content(self.image_download_chunk_size):
                                    f.write(chunk)
                else:
                    print("Skip downloading images.")

            data_row['image-links'] = json.dumps(images_links_list, ensure_ascii=False)

        return data_row['image-links']

    @staticmethod
    def extract_specs(data_row, item_page_content, download_path):
        # extract data/specs and add as a string field
        specs_pattern = re.compile(
            r'<div\sclass=\"c-params__list-key.*?\"><span\sclass=\"block.*?\">('
            r'.*?)</span></div><div\sclass=\"c-params__list-value.*?\"><span\sclass=\"block.*?\">('
            r'.*?)</span></div>',
            flags=re.DOTALL)
        matches = specs_pattern.findall(item_page_content)
        specs_list = []
        spec_row = {'spec_key': '', 'spec_value': ''}
        for spec in matches:
            spec_row['spec_key'] = spec[0].strip()
            spec_row['spec_value'] = spec[1].strip()
            specs_list.append(copy.deepcopy(spec_row))
        spec_string = json.dumps(specs_list, ensure_ascii=False)
        data_row['specs'] = spec_string
        return spec_string

    def _item_exist(self, product_id):
        if (self.all_data['product-id'] == product_id).any():
            return True
        else:
            return False

    def start(self, starting_page=None, ending_page=None, item_per_page=None, download_images=False,
              download_images_only_if_not_exist=True):
        if starting_page is not None:
            self.page_num = starting_page
        if ending_page is not None:
            self.max_page_num = ending_page
        if item_per_page is not None:
            self.item_per_page = item_per_page
        self.load_data()
        item_cnt = (self.page_num - 1) * self.item_per_page + 1
        while self.page_num <= self.max_page_num:
            print("Crawling page = %d" % self.page_num)
            params = {'pageno': self.page_num, 'sortby': self.sort_by}
            response = self.http.get(url=self.list_item_page_url, params=params)
            page_content = response.text
            items = pd.DataFrame()

            items_common_pattern = re.compile(r'is-plp.*?data-observed=\"(.*?)\".*?data-index=\"(.*?)\".*?data-id=\"('
                                              r'.*?)\".*?data-price=\"(.*?)\".*?data-title-fa=\"('
                                              r'.*?)\".*?data-title-en=\"(.*?)\".*?data-enhanced-ecommerce=\'({'
                                              r'.*?})\'.*?data-id=\"(.*?)\".*?(data-snt-params=\'({.*?})\'.*?href=\"('
                                              r'.*?)\".*?src=\"(.*?)\".*?alt=\"(.*?)\")', flags=re.DOTALL)

            matched_items = items_common_pattern.findall(page_content)

            ## seller & guaranty info
            items_seller_guarantee_pattern = re.compile(
                r'seller-link\"\shref=\"(.*?)\">.*?</span>(.*?)</span>.*?\"productId\":(.*?),.*?seller-text\">('
                r'.*?)<.*?guarantee\">(.*?)<',
                flags=re.DOTALL)
            with_price_matched_items = items_seller_guarantee_pattern.findall(page_content)
            if not with_price_matched_items:
                pass

            data_row = {
                'product-id': 0,
                'title': '',
                'price': 0,
                'category': '',
                'variant': 0,
                'brand': '',
                'link': '',
                'seller-link': '',
                'seller-name': '',
                'guarantee': '',
                'quantity': 0,
                'position': 0,
                'first-image-link': '',
                'specs': '',
                'image-links': '',
            }

            page_item_cnt = 1

            for item in matched_items:
                # check item already exist
                data_row = self.extract_basic_data(item)
                print("page=%d, item.no=%d, item.no.total=%d, link=%s" %
                      (self.page_num, page_item_cnt, item_cnt, data_row['link']))

                if not self._item_exist(data_row['product-id']):
                    data_row = self.extract_full_data(item, with_price_matched_items)
                    response = self.http.get(url=self.product_detail_link_pattern % data_row['product-id'])
                    item_page_content = response.text

                    # create a directory with product-id, if it does not exist
                    base_path = os.path.dirname(__file__)
                    dir_abs_path = os.path.join(base_path, self.results_path, str(data_row['product-id']))
                    Path(dir_abs_path).mkdir(parents=True, exist_ok=True)

                    # save html content
                    html_abs_path = os.path.join(dir_abs_path, str(data_row['product-id']) + '.html')
                    with open(html_abs_path, 'w', encoding='utf-8') as fp:
                        fp.write(item_page_content)

                    data_row['specs'] = self.extract_specs(data_row, item_page_content, dir_abs_path)

                    data_row['image-links'] = self.download_images(data_row, item_page_content, dir_abs_path)

                    # save data as csv file
                    csv_abs_path = os.path.join(dir_abs_path, str(data_row['product-id']) + '.csv')
                    item_df = pd.DataFrame(columns=self.columns)
                    item_df = item_df.append(data_row, ignore_index=True)
                    item_df.to_csv(path_or_buf=csv_abs_path, encoding='utf-8', index=False)

                    items = items.append(data_row, ignore_index=True)

                    # append to main dataframe
                    self.all_data = self.all_data.append(data_row, ignore_index=True)
                    page_item_cnt += 1
                    item_cnt += 1

                    # save list page data
                    self.all_data.to_csv(path_or_buf=self.all_data_file_abs_path, encoding='utf-8', index=False)

                # data_row already exist. Fetch it and download images
                elif self.download_images_only_if_not_exist:
                    print("Item's data already exists. Downloading images...")
                    fetched_data_row = self.all_data.loc[self.all_data['product-id'] == data_row['product-id']]
                    data_row = fetched_data_row.to_dict('records')[0]

                    # create a directory with product-id, if it does not exist
                    base_path = os.path.dirname(__file__)
                    dir_abs_path = os.path.join(base_path, self.results_path, str(data_row['product-id']))
                    Path(dir_abs_path).mkdir(parents=True, exist_ok=True)
                    links = self.download_images(data_row=data_row, item_page_content='', download_path=dir_abs_path)

                    page_item_cnt += 1
                    item_cnt += 1
                else:
                    print("Item already exists!")
                    page_item_cnt += 1
                    item_cnt += 1

            self.page_num += 1

