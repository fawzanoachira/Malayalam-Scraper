"""
Module: Web Scraping with concurrency

[imports]
Scrapy: Scrapy is a comprehensive web crawling and web scraping framework for Python. 
        It provides tools and libraries for building web spiders that can navigate websites, 
        extract data, and follow links. It includes features like spiders, selectors, pipelines, middleware, and more.
scrapy.linkextractors.LinkExtractor: LinkExtractor is a class from the Scrapy library, used for extracting links from 
        web pages. It helps you find and follow links on a website during web scraping. You can configure it to extract 
        links based on various criteria like URL patterns, domains, or link attributes.
datetime: This module is a part of Python's standard library. It provides classes for working with dates and times. 
        In your code, you're importing it to work with dates and times, possibly for timestamping or 
        date-related operations.
re: This module is Python's regular expression library. It allows you to work with regular expressions, which are
         patterns used for string matching and manipulation. In your code, it may be used for text parsing or 
         pattern matching.
bs4 (Beautiful Soup): Beautiful Soup is a popular Python library used for web scraping and parsing HTML and 
        XML documents. It provides convenient methods for navigating and searching the elements of web pages, 
        making it easier to extract data from HTML documents.
concurrent.futures: The concurrent.futures module is part of the Python standard library and provides a high-level 
        interface for asynchronously executing functions or methods using threads or processes. It's often used for 
        parallelism and asynchronous programming, which can be helpful for tasks like making multiple HTTP requests 
        in parallel.
requests: The requests library is a widely used Python library for making HTTP requests. It simplifies the process 
        of sending HTTP requests and handling responses, making it a common choice for web scraping tasks where you 
        need to fetch web pages.


[constants]
NUM_THREADS: Defines the number of threads for concurrent scraping


[functions]
remove_tags:This function is designed to remove HTML tags from a given HTML content and extract the text content.
            It does the following:
            - It uses the Beautiful Soup library to parse the HTML content.
            - It removes style and script tags from the parsed content.
            - It extracts and joins the text content from the remaining HTML tags, using a regular expression to 
              filter out specific characters. The filtered text is returned.
scrape_page:This function is designed to scrape a single web page. 
            It takes a URL as an argument and does the following:
            - Sends an HTTP GET request to the specified URL using the requests library.
            - Checks if the response status code is 200 (indicating a successful request).
            - Uses Beautiful Soup to parse the HTML content, extracts text content, and saves 
              it to a file with a timestamp.

              
[class]
QuotesSpider (Scrapy Spider):This is a Scrapy spider class used for crawling and scraping web pages. 
            It inherits from the Scrapy Spider class and contains the following key components:
            - name: Defines the name of the spider.
            - start_urls: Specifies the initial URLs to scrape.
            - parse(self, response): This is the method that gets called to process the response from the crawled URL. 
              It does the following:
              - Calls the remove_tags function to clean the HTML content and extract text.
              - Checks if the extracted text is substantial (more than 50 characters) and, if so, saves it to a file 
                with a timestamp.
              - Follows links on the page by using the LinkExtractor class, and for each link, it recursively calls 
                the parse method for further processing.

Note: Some changes were made in the settings which include the following:
        CONCURRENT_REQUESTS = 32
        CONCURRENT_REQUESTS_PER_DOMAIN = 8
        CONCURRENT_REQUESTS_PER_IP = 8
"""


# Import necessary libraries
from scrapy.linkextractors import LinkExtractor
import scrapy
from datetime import datetime
import re, os
import numpy as np
from bs4 import BeautifulSoup
import concurrent.futures
import requests
import pytesseract
from pdf2image import convert_from_path, convert_from_bytes
from urllib.parse import urlparse
import csv
from PIL import Image


# Define the number of threads for concurrent scraping
NUM_THREADS = 4

# Create a list of URLs to be scraped
urls_to_scrape = [
    # "your-link"
]

def download_pdf(url, filename):
    print("Downloading the PDF", url.split('/')[-1])
    response = requests.get(url)
    with open(filename, 'wb') as f:
        f.write(response.content)
    return response.status_code

def convert_pdf_to_text(filename, i):
    try:
        pdf = convert_from_path(filename)
        for img in pdf:
            # img.save(f"/home/fawzan/Projects/Malayalam Scraper/tutorial/images/output_image{i}.jpg", 'JPEG')
            text += pytesseract.image_to_string(np.array(img), lang = 'mal+eng')
            # image_filename = f"/home/fawzan/Projects/Malayalam Scraper/tutorial/images/output_text{i}.txt"
            # with open(image_filename, "a") as f:
            #     f.write(text)
            # i += 1
            # os.remove(image)
        return text
    except Exception as e:
        print(e)

# Function to remove HTML tags from a given HTML content
def remove_tags(html:str)-> str:
    """
    Removes HTML tags and extracts text content from the provided HTML.
    
    Args:
        html (str): The HTML content to process.
        
    Returns:
        str: The extracted text content without HTML tags.
    """
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Remove style and script tags from the parsed content
    for data in soup(["style", "script"]):
        data.decompose()
    
    pdf_text = ''

    for a in soup.find_all('a', href=True):
        print ("Found the URL:", a['href'])
        filename = f"/home/fawzan/Projects/Malayalam Scraper/tutorial/file_res/{datetime.now()}.pdf"
        if a['href'].endswith(".pdf"):
            file_response_status_code = download_pdf(a['href'] , filename)
            # if file_response_status_code == 200:
            pdf_text += convert_pdf_to_text(filename, 1)

    # Extract and join the text content from the remaining tags
    return " ".join(re.findall(r'[\u0D00-\u0D7F]+', " ".join(soup.stripped_strings)))

# Define a Scrapy spider to crawl and scrape web pages
class QuotesSpider(scrapy.Spider):
    name = "quotes"
    start_urls = urls_to_scrape

    def save_metadata_to_csv(self, metadata):
        with open('metadata.csv', 'a', newline='') as csvfile:
            fieldnames = ['filename', 'domain_name', 'date_time_scraped', 'url']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write header if the file is empty
            if csvfile.tell() == 0:
                writer.writeheader()

            writer.writerow(metadata)

    def parse(self, response):
        """
        Parses the response from a URL and extracts text content from it.
        
        Args:
            response (scrapy.http.Response): The response from the crawled URL.
        """
        # Remove HTML tags and extract text content from the response
        text = remove_tags(response.text)

        # Check if the extracted text is substantial (more than 50 characters)
        if len(text) > 50:

            time = datetime.now()
            filename = f"res/{time}.txt"
            domain_name = urlparse(response.url).netloc
            date_time_scraped = time.strftime('%Y-%m-%d %H:%M:%S')

            # Save the extracted text to a file named with the current timestamp
            with open(filename, "w+") as f:
                f.write(text)
            
            # Extracted metadata to be saved in CSV
            metadata = {
                'filename': filename,
                'domain_name': domain_name,
                'date_time_scraped': date_time_scraped,
                'url': response.url,
            }

            # Save metadata to CSV
            self.save_metadata_to_csv(metadata)

        # Follow links on the page and recursively call the 'parse' method for each link
        for a in LinkExtractor(deny_domains="").extract_links(response):    
            yield response.follow(a, callback=self.parse)
            yield response.follow(a)
            print(a.url)

# Function to scrape a single page and save its text content to a file
def scrape_page(url:str)->None:
    """
    Scrapes a single web page, extracts text content, and saves it to a file.
    
    Args:
        url (str): The URL of the web page to scrape.
    """
    try:
        # Send an HTTP GET request to the URL
        response = requests.get(url)

        # Check if the response status code is 200 (indicating a successful request)
        if response.status_code == 200:
            # Parse the HTML content using BeautifulSoup and extract text content
            soup = BeautifulSoup(response.text, "html.parser")
            text = " ".join(re.findall(r'[\u0D00-\u0D7F]+', " ".join(soup.stripped_strings)))

            # Save the extracted text to a file named with the current timestamp
            with open(f"res/{datetime.now()}.txt", "w+") as f:
                f.write(text)
    except Exception as e:
        print('Error', e)

# Use concurrent.futures to scrape the URLs concurrently using multiple threads
with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
    # Map the 'scrape_page' function to each URL for concurrent scraping
    executor.map(scrape_page, urls_to_scrape)


# print(""+a.url)
            # x = a.url.split("/", 3)
            # # if x[2].endswith(":"):
            # if a.url.endswith(".pdf"):
            #         url = x
            #         filename = f"{url[-20:-4]}.pdf"
            #         self.download_pdf(url, filename)
            #         self.convert_pdf_to_text(filename)
            # else:
