import requests
from requests import Response
from bs4 import BeautifulSoup, Tag
from requests.compat import urljoin, urlparse
import os
from pathlib import Path
import json

class InsuranceCompanySite:
    """ Insurance Company WWW site which will be scrapped. """
    def __init__(self, url: str, max_depth: int = 2) -> None:
        """
        Parameters
        ----------
            url: str
                The main page of site
            max_depth: int
                How many steps from the main page
        """
        self.urls = set([url])
        self.depth = 0
        self.max_depth = max_depth
        self.already_visited_url = set()
        self.visited = dict()
        self.domains = set()
        self.skipped_domains = set(['itunes.apple.com', 'apps.apple.com', 'play.google.com', 'play.google.com', 'www.linkedin.com', 'www.youtube.com', 'www.amice-eu.org', 'www.gov.pl', 'piu.org.pl'])
        self.output = []
        self.output_file = self.crete_file_name(url)
    
    def crete_file_name(self, url: str) -> str:
        """ Creates valid file name from the main page url address

        Parameters
        ----------
            url: str
        
        Returns
        -------
            str
                Valid file name with '.json' extension
        """
        ret = urlparse(url).netloc
        ret = ret.removeprefix("www.")
        ret = ret.removesuffix(".pl")
        ret = ret.removesuffix(".com")
        return os.path.join("data", ret + ".json")
        
    def process(self):
        """ Starts processig site """
        while self.depth < self.max_depth and len(self.urls) > 0:
            out_urls = set()
            for parent_url in self.urls:
                url = parent_url.split(" <- ")[0]
                # print(url)
                try:
                    response = requests.get(url)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    self.already_visited_url.add(response.url)
                    self.visited[response.url] = self.filter_headers(response.headers)
                    for link in soup.find_all('a'):
                        url = urljoin(response.url, link.get('href'))
                        domain = urlparse(url).netloc
                        child_url = "{} <- {}".format(url, parent_url)
                        if not(domain in self.skipped_domains) and not(url in self.already_visited_url) and not(url in out_urls):
                            self.process_link(link, child_url, response)
                            out_urls.add(child_url)
                            self.domains.add(domain)
                except Exception as e:
                    print(e)
            self.urls = out_urls
            self.depth += 1
        print(sorted(self.domains))
        self.save_data()
        print(self.visited)

    def process_link(self, link: Tag, child_url, response: Response):
        """ Process one html <a> tag - link
        
        Parameters
        ----------
            link: Tag
                whole <a> html Tag

        """
        url = urljoin(response.url, link.get('href'))
        text = link.get_text().replace("\n", " ").strip()
        if self.is_owu(url, text):
            print("{} {}: {} -> {}".format(self.depth, response.url, text, url))
            headers = self.download_file(url)
            urls = child_url.split(" <- ")
            self.output.append({ "text": text, "url": urls[0], "parents": urls[1:], "headers": self.filter_headers(headers) })

    def is_owu(self, url: str, text: str) -> bool:
        """ Determines if it is OWU - text contains 'warunki' and:
        - url ends with '.pdf'
        - response content-type is 'application/pdf'
        
        Parameters
        ----------
            url: str
                Pure URL address
            text: str
                Text - body of html <a> tag

        Returns
        -------
            bool
                True if it is OWU
        """
        if text.lower().count("warunki") > 0:
            print(text, url)
            if url.lower().endswith(".pdf"):
                return True
            else:
                r = requests.head(url)
                if r.headers['Content-Type'] == 'application/pdf':
                    return True
        return False

    def download_file(self, url: str):
        """ Download PDF file and writes it in directory
        named like the main page address
        
        Parametes
        ---------
            url: str
                Address of the PDF file
        
        Returns
        -------
            headers: CaseInsensitiveDict[str]
                Response headers 
        """
        directory = self.output_file.removesuffix(".json")
        Path(directory).mkdir(parents=True, exist_ok=True)
        local_filename = os.path.join(directory, url.split('/')[-1])
        if '.' not in local_filename:
            local_filename = local_filename + '.pdf'
        print(local_filename)
        # NOTE the stream=True parameter below
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)                
            return r.headers

    def filter_headers(self, headers):
        """ Filters response headers which are important to
        detect further changes """
        ret = dict()
        ret["Content-Type"] = headers["Content-Type"]
        ret["Content-Length"] = headers["Content-Length"]
        if "Last-Modified" in headers:
            ret["Last-Modified"] = headers["Last-Modified"]
        if "Expires" in headers:
            ret["Expires"] = headers["Expires"]
        return ret
    
    def save_data(self):
        """ Saves output JSON file with data of scapped documents """
        with open(self.output_file, 'tw', encoding="UTF-8") as outfile:
            json.dump(self.output, outfile, indent=4, ensure_ascii=False)