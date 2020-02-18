import argparse
import re
from pprint import pprint
from sys import argv

import pandas as pd
import requests
from bs4 import BeautifulSoup


def get_args(args):
    parser = argparse.ArgumentParser()
    parser.description = 'Cloud Pricing Crawler.'

    parser.add_argument('--print', action='store_true', default=False,
                        help='Print results in the screen.')
    parser.add_argument('--save_csv', action='store_true', default=False,
                        help='Save the data in csv file.')
    parser.add_argument('--save_json', action='store_true', default=False,
                        help='Save the data in json file.')

    return parser.parse_args(args)


class AbstractCrawler():
    def __init__(self, url, enterprise=None, bs4_parser='html.parser',
                 columns=None, ignore_error=False, break_on_error=False):
        self.url = url
        self.soup = BeautifulSoup(self.get_content(), bs4_parser)
        self.enterprise = enterprise
        self._columns = ['Storage', 'Storage unit', 'Storage type', 'CPU',
                         'Memory', 'Memory unit', 'Bandwidth', 'Bandwidth unit', 'Price mo']

        if columns:
            self._columns = columns

        self.ignore_error = ignore_error
        self.break_on_error = break_on_error

        self._df = pd.DataFrame(columns=self._columns)
        self.update_data()

    def get_content(self):
        request = requests.request('GET', self.url)
        if request.status_code == 200:
            return request.content
        else:
            raise Exception('Bad request! Status: %s' % request.status_code)

    def get_content_data_table(self):
        raise NotImplementedError

    def col_process(self, col):
        col = re.sub(r'([\n\t])', '', ' '.join(col)).strip()
        col = re.sub(r' {2}', ' ', col)

        return col

    def row_process(self, row):
        raise NotImplementedError

    def drop_data(self):
        self._df.drop(self._df.index, inplace=True)

    def update_data(self):
        df = pd.DataFrame(columns=self._columns)
        data = self.get_content_data_table()

        for row_idx, row in enumerate(data):
            for col_idx, col in enumerate(row):
                try:
                    col = self.col_process(col)
                finally:
                    row[col_idx] = col

            try:
                df_row = self.row_process(row)
                assert len(df_row) == len(self._columns), \
                    'The row_process must return a list with %s columns.' % len(self._columns)

                df = df.append({k: df_row[i] for i, k in enumerate(self._columns)}, ignore_index=True)
            except Exception as e:
                if not self.ignore_error: print(e)
                if self.break_on_error: raise e

        if not df.empty:
            self.drop_data()
            self._df = df.copy()
            if 'Enterprise' in self._df.columns:
                self._df['Enterprise'] = self.enterprise
            else:
                self._df.insert(0, 'Enterprise', self.enterprise, True)

    @property
    def data(self) -> pd.DataFrame:
        return self._df

    @property
    def columns(self) -> list:
        return self._columns

    @columns.setter
    def columns(self, columns: list):
        if len(columns) == len(self._columns):
            self._columns = columns
        else:
            raise Exception('The length of columns must be %s.' % len(self._columns))


class VulturCrawler(AbstractCrawler):
    def __init__(self, bs4_parser='html.parser',
                 columns=None, ignore_error=False, break_on_error=False):
        super(VulturCrawler, self).__init__(
            url='https://www.vultr.com/pricing/', enterprise='Vultur',
            bs4_parser=bs4_parser, columns=columns, ignore_error=ignore_error, break_on_error=break_on_error
        )

    def get_content_data_table(self):
        soup = self.soup.body.find_all('div', {'class': 'pt__row-content'})
        soup = [s.find_all('div', {'class': re.compile(r'pt__cell')}) for s in soup]
        data_body = [[list(span.strings) for span in span_list] for span_list in soup]

        return data_body

    def row_process(self, row):
        # geekbench_score = row[0].rsplit(' ', 1)[1]
        # geekbench_score = int(geekbench_score) if geekbench_score.isdigit() else None

        storage, storage_unit, storage_type = row[1].split(' ')
        storage = int(storage.replace(',', ''))

        cpu = int(row[2].split(' ')[0])

        memory, memory_unit = row[3].split(' ')[:2]
        memory = int(memory)

        bandwidth, bandwidth_unit = row[4].split(' ')[:2]
        bandwidth = float(bandwidth)
        # bandwidth_ipv6 = len(re.findall(r'ipv6', row[4].lower())) > 0

        price_mo, price_hr = re.findall(r'\d+[.,]*\d*', row[5])
        price_mo = float(price_mo)
        price_hr = float(price_hr)

        df_row = [
            storage, storage_unit, storage_type,
            cpu,
            memory, memory_unit,
            bandwidth, bandwidth_unit,
            price_mo
        ]

        return df_row


class DigitalOceanCrawler(AbstractCrawler):
    def __init__(self, bs4_parser='html.parser',
                 columns=None, ignore_error=False, break_on_error=False):
        super(DigitalOceanCrawler, self).__init__(
            url='https://www.digitalocean.com/pricing/', enterprise='Digital Ocean',
            bs4_parser=bs4_parser, columns=columns, ignore_error=ignore_error, break_on_error=break_on_error
        )

    def get_content_data_table(self):
        tr_list = self.soup.find('div', {'id': 'standard-droplets-pricing-table'}).find('tbody').find_all('tr')
        data = [[list(td_list.strings) for td_list in tr.find_all('td')] for tr in tr_list]

        return data

    def row_process(self, row):
        memory, memory_unit = row[0].split(' ')

        cpu = row[1].split(' ')[0]

        bandwidth, bandwidth_unit = row[2].split(' ')

        storage, storage_unit = row[3].split(' ')
        storage = int(storage.replace(',', ''))
        storage_type = 'SSD'

        price_mo, price_hr = re.findall(r'\d+[.,]*\d*', row[4])
        price_mo = float(price_mo)
        # price_hr = float(price_hr)

        df_row = [
            storage, storage_unit, storage_type,
            cpu,
            memory, memory_unit,
            bandwidth, bandwidth_unit,
            price_mo
        ]

        return df_row


def digital_ocean_standard_pricing_crawler():
    url = 'https://www.digitalocean.com/pricing/'
    request = requests.request('GET', url)
    soup = BeautifulSoup(request.content, 'html.parser')

    tr_list = soup.find('div', {'id': 'standard-droplets-pricing-table'}).find('tbody').find_all('tr')
    data = [[list(td_list.strings) for td_list in tr.find_all('td')] for tr in tr_list]


if __name__ == '__main__':
    args = get_args(argv[1:])
    vultur_crawler = VulturCrawler(break_on_error=1)
    digital_ocean_crawler = DigitalOceanCrawler(break_on_error=1)

    data = vultur_crawler.data.append(digital_ocean_crawler.data, ignore_index=True)
    if args.print: pprint(data)
    if args.save_csv: data.to_csv('data.csv', sep=';', index=False)
    if args.save_json: data.to_json('data.json', orient='records')
