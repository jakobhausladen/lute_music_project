import os
import pandas as pd
import json
import re
from fuzzywuzzy import fuzz
from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess

# dictionaries to store the scraped data
composer_data_classical = {}
composer_data_musicalics = {
    'birth': {},
    'group': {},
    'death': {}
}

class ComposersClassicalMusicSpider(Spider):
    name = 'composer_classical_spider'

    def __init__(self, composers, *args, **kwargs):
        super(ComposersClassicalMusicSpider, self).__init__(*args, **kwargs)
        self.composers = composers
        # construct search links for composers
        self.composer_search_links = [
            f'http://search.freefind.com/find.html?id=596354&pageid=r&mode=ALL&query={comp.replace(' ', '+')}'
            for comp in composers
        ]

    # for each composer, feed index and search link to parse_search method
    def start_requests(self):
        for index, link in enumerate(self.composer_search_links):
            yield Request(url=link, callback=self.parse_search, cb_kwargs=dict(index=index))

    def parse_search(self, response, index):
        # get search result font elements of the first page
        search_results = response.xpath('.//font[@class="search-results"]/a').extract()
        # get best fuzzy match
        max_score = 0
        max_index = -1
        for string_index, string in enumerate(search_results):
            score = fuzz.token_set_ratio(self.composers[index], string)
            if max_score < score:
                max_score = score
                max_index = string_index

        if max_index != -1:
            # construct site link for best match
            site_link = response.xpath(f'.//font[@class="search-results"][{max_index + 1}]/a/@href').get()
            # if match is good enough, feed site link and index to parse_composer_site method
            if max_score > 90:
                yield response.follow(url=site_link, callback=self.parse_composer_site, cb_kwargs=dict(index=index))

    # extract first text in the font element and save it in the composer_data dict
    def parse_composer_site(self, response, index):
        info = response.xpath('.//font/text()[1]').extract_first()
        if info:
            info = re.sub('\n', ' ', info)
            composer_data_classical[self.composers[index]] = info


class MusicalicsSpider(Spider):
    name = 'composer_musicalics_spider'

    def __init__(self, composers, *args, **kwargs):
        super(MusicalicsSpider, self).__init__(*args, **kwargs)
        self.composers = composers
        # construct search links for composers
        self.composer_search_links = [
            f'https://musicalics.com/en/search/composer/{comp.replace(" ", "%2520")}'
            for comp in composers
        ]

    # for each composer, feed index and search link to parse_search method
    def start_requests(self):
        for index, link in enumerate(self.composer_search_links):
            yield Request(url=link, callback=self.parse_search, cb_kwargs=dict(index=index))

    def parse_search(self, response, index):
        # get search result names of the first page
        group_header_texts = response.xpath('.//*[@class="group-header"]//text()').extract()
        possible_comps = [x for x in group_header_texts if re.search('\n|\s\s', x) is None]
        # get best fuzzy match
        max_score = 0
        max_comp = None
        for comp in possible_comps:
            score = fuzz.token_set_ratio(comp, self.composers[index])
            if max_score < score:
                max_score = score
                max_comp = comp

        if max_comp:
            # construct site link for best match
            composer_site_link = 'https://musicalics.com' + response.xpath(f'.//a[contains(text(),"{max_comp}")]/@href').get()
            # if match is good enough, feed site link and index to parse_composer_site method
            if max_score > 90:
                yield response.follow(url=composer_site_link, callback=self.parse_composer_site, cb_kwargs=dict(index=index))

    # extract all text in the left-, middle-, and right-group divs and save it in the composer_data dict
    def parse_composer_site(self, response, index):
        composer_data_musicalics['birth'][self.composers[index]] = response.xpath('.//div[@class="group-left"]//text()').extract()
        composer_data_musicalics['group'][self.composers[index]] = response.xpath('.//div[@class="group-middle"]//text()').extract()
        composer_data_musicalics['death'][self.composers[index]] = response.xpath('.//div[@class="group-right"]//text()').extract()


# function to run spiders
def run_spiders(spider_list, composers):
    process = CrawlerProcess()
    for spider in spider_list:
        process.crawl(spider, composers=composers)
    process.start()


if __name__ == '__main__':
    # load cleaned comoposer names
    wd = os.getcwd()
    path_cleaned = os.path.join(wd, 'data', 'data_sets', 'cleaned_composers.csv')
    df_comp_cleaned = pd.read_csv(path_cleaned, encoding="latin-1")
    composers = df_comp_cleaned.cleaned_name.unique().tolist()

    # small example composer list
    composers = ['John Dowland', 'Francesco da Milano', 'Nicolas Vallet', 'Hans Newsidler']

    # run spiders
    run_spiders(
        spider_list=[ComposersClassicalMusicSpider, MusicalicsSpider],
        composers=composers
    )

    # export scraped data as JSON
    with open(os.path.join(wd, 'data', 'data_sets', 'composer_data_classical.json'), 'w', encoding='utf-8') as f:
        json.dump(composer_data_classical, f, ensure_ascii=False, indent=4)

    with open(os.path.join(wd, 'data', 'data_sets', 'composer_data_musicalics.json'), 'w', encoding='utf-8') as f:
        json.dump(composer_data_musicalics, f, ensure_ascii=False, indent=4)

 