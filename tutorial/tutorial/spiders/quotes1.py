import scrapy


class Quotes1Spider(scrapy.Spider):
    name = "quotes1"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["https://quotes.toscrape.com"]

    def parse(self, response):
        pass
