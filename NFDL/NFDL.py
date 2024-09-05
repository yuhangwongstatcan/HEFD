import urllib.request as urllib2
import requests
import os
import time


from urllib.parse import urlparse
from bs4 import BeautifulSoup

year_to_scan = ["2014","2015","2016","2017","2018","2019","2020","2021","2022","2023","2024"]
month_to_scan = ["01","02","03","04","05","06","07","08","09","10","11","12"]

possible_url = ['2024_01.php', '2024_02.php', '2023_01.php', '2023_02.php', '2023_03.php', '2023_04.php', '2023_05.php', '2023_06.php', '2023_07.php', '2023_08.php', '2023_09.php', '2023_10.php', '2023_11.php', '2023_12.php', '2022_01.php', '2022_02.php', '2022_03.php', '2022_04.php', '2022_05.php', '2022_06.php', '2022_07.php', '2022_08.php', '2022_09.php', '2022_10.php', '2022_11.php', '2022_12.php', '2021_01.php', '2021_02.php', '2021_03.php', '2021_04.php', '2021_05.php', '2021_06.php', '2021_07.php', '2021_08.php', '2021_09.php', '2021_10.php', '2021_11.php', '2021_12.php', '2020_01.php', '2020_02.php', '2020_03.php', '2020_04.php', '2020_05.php', '2020_06.php', '2020_07.php', '2020_08.php', '2020_09.php', '2020_10.php', '2020_11.php', '2020_12.php', '2019_01.php', '2019_02.php', '2019_03.php', '2019_04.php', '2019_05.php', '2019_06.php', '2019_07.php', '2019_08.php', '2019_09.php', '2019_10.php', '2019_11.php', '2019_12.php', '2018_01.php', '2018_02.php', '2018_03.php', '2018_04.php', '2018_05.php', '2018_06.php', '2018_07.php', '2018_08.php', '2018_09.php', '2018_10.php', '2018_11.php', '2018_12.php', '2017_01.php', '2017_02.php', '2017_03.php', '2017_04.php', '2017_05.php', '2017_06.php', '2017_07.php', '2017_08.php', '2017_09.php', '2017_10.php', '2017_11.php', '2017_12.php', '2016_01.htm', '2016_02.htm', '2016_03.htm', '2016_04.htm', '2016_05.htm', '2016_06.htm', '2016_07.htm', '2016_08.htm', '2016_09.htm', '2016_10.htm', '2016_11.htm', '2016_12.php', '2015Jan.html', '2015Feb.html', '2015Mar.html', '2015Apr.html', '2015May.html', '2015Jun.html', '2015Jul.html', '2015Aug.html', '2015Sep.htm', '2015Oct.htm', '2015Nov.htm', '2015Dec.htm', '2014Jan.html', '2014Feb.html', '2014Mar.html', '2014Apr.html', '2014May.html', '2014Jun.html', '2014Jul.html', '2014Aug.html', '2014Sep.html', '2014Oct.html', '2014Nov.html', '2014Dec.html']

url = 'http://www.pub.nl.ca/applications/IslandInterconnectedSystem/DemandStatusReports.php'

def readMthHTML(url):
    print(url)
    # Fetch the html file
    response = urllib2.urlopen(url)
    mthhtml_doc = response.read()
    # Parse the html file
    soup = BeautifulSoup(mthhtml_doc, 'html.parser')

    # Format the parsed html file
    # strhtm = soup.prettify()

    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    urls = []
    for link in soup.find_all('a'):
        if '.pdf' in link.get('href'):
            print(link.get('href'))
            a = urlparse(link.get('href'))
            filename = os.path.basename(a.path)
            print(filename)
            downloadPDF(filename, "http://www.pub.nl.ca/applications/IslandInterconnectedSystem/" + link.get('href'))
            time.sleep(10) # Sleep for 10 seconds

def downloadPDF(filename, url):
    r = requests.get(url, allow_redirects=True)
    open('./dwldfiles/' + filename, 'wb').write(r.content)

# Fetch the html file
response = urllib2.urlopen(url)
html_doc = response.read()

# Parse the html file
soup = BeautifulSoup(html_doc, 'html.parser')

# Format the parsed html file
# strhtm = soup.prettify()

reqs = requests.get(url)
soup = BeautifulSoup(reqs.text, 'html.parser')
 
for link in soup.find_all('a'):
    if link.get('href') in (possible_url):
        readMthHTML("http://www.pub.nl.ca/applications/IslandInterconnectedSystem/" + link.get('href'))