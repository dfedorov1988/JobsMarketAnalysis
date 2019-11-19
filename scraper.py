import json
import re

import scrapy
import scrapy.settings
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging, logging

configure_logging(install_root_handler=True)
# suppress debugging logs
logging.disable(50)  # CRITICAL = 50


def construct_url(url_start, pos, loc):
    # getting rid of punctuation in query
    job_q = re.sub(r"[,.;@#?!&$\ *]+\ *", "+", pos.strip())
    location_q = re.sub(r"[,.;@#?!&$\ *]+\ *", "+", loc.strip())
    # final website to scrape
    url_full = url_start + "/jobs?q=" + job_q + "&l=" + location_q

    return url_full


# Create the Spider class
class IndeedSpider(scrapy.Spider):
    name = "indeed_spider"

    # start_requests method
    def start_requests(self):
        for url in urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse_pages)

    def parse_pages(self, response):
        # print("Existing settings: %s" % self.settings.attributes['AUTOTHROTTLE_DEBUG'])
        # Create a SelectorList jobs postings on the page
        # this goes through job posting cards on the search results page
        for job_title in response.css("div.jobsearch-SerpJobCard"):
            # Extract the text and strip it clean
            job_title_ext = job_title.css('div.title > a ::attr(title)').extract_first().strip()

            # Create a SelectorList of course descriptions text
            job_link = job_title.css('div.title > a::attr(href)').extract_first().strip()
            # extracting unique job id
            job_id = job_title.css('div.title > a::attr(id)').extract_first().strip()

            company = job_title.css('span.company::text').extract_first()
            if company.strip():
                company.strip()
            if not company.strip():
                company = job_title.css('span.company > a::text').extract_first().strip()
            print("Company:", company)
            location = job_title.css('.location ::text').extract_first().strip()
            # print("Company:", company, " Location:", location)

            # This makes sure we have unique ID for every posting
            # Need to get rid of duplicates later since jobs might show up
            # on different pages with different IDs
            full_id = job_title_ext + "_id_" + job_id

            # Fill in the dictionary
            full_url = url_start + job_link
            # print("Job:", job_title_ext.strip(), company.strip(), location.strip())
            # saving what we found on the page with postings
            job_dict = {'title': job_title_ext,
                        'link': full_url,
                        'company': company,
                        'location': location,
                        'id': full_id,
                        }

            request = response.follow(full_url, callback=self.parse_job_contents)
            # saving job title as metadata in response
            request.meta['dict'] = job_dict
            yield request

        # This sends the scraper to the next page
        next_page = response.css('.pagination > a:last-of-type::attr(href)').extract_first()
        print("NEXT_PAGE:", next_page)
        if next_page:
            url = response.urljoin(next_page)
            print("URL:", url)
            yield scrapy.Request(url, self.parse_pages)

    def parse_job_contents(self, response):
        """Pulls job description and writes it to a file
        File is names after the position title + _id_
        to avoid overwriting because positions might have the same name"""

        # for now keeping html tags in postings. They might be useful?
        job_descr = response.css('div.jobsearch-jobDescriptionText *::text').extract()

        # pulling dictionary with the job posting from metadata
        job_dict = response.meta['dict']

        # adding job description to dict
        job_dict['description'] = job_descr
        full_id = job_dict['id']
        # This is dictionary with unique job postings, contains a dictionary for
        # each posting, adding job dict to the job_dicts
        jobs_dict[full_id] = job_dict

        yield {'job_description': job_descr}


url_start = 'https://www.indeed.com'

# List of all American States
# Might want to do California search by city and remove duplicates later
# Indeed only shows 100 pages per search, might not be enough
states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

job_to_search = 'Data Scientist'
# location_to_search = 'San Francisco'
urls = []
for location_to_search in states:
    url_full = construct_url(url_start, job_to_search, location_to_search)
    urls.append(url_full)

# Initialize the dictionary **outside** of the Spider class
jobs_dict = {}
current_state = 0
# Run the Spider
process = CrawlerProcess()
process.crawl(IndeedSpider)
process.start()

print("Number of job postings found:", len(jobs_dict))

# Saving jobs to json
# Perhaps need to get rid of duplicates at this point
# json_name = re.sub(r"[,.;@#?!&$\ *]+\ *", "_", job_to_search.strip()) \
#             + "_" \
#             + re.sub(r"[,.;@#?!&$\ *]+\ *", "_", location_to_search.strip()) \
#             + ".json"
json_name = re.sub(r"[,.;@#?!&$\ *]+\ *", "_", job_to_search.strip()) + "_USA.json"

with open(json_name, 'w') as fp:
    json.dump(jobs_dict, fp)
