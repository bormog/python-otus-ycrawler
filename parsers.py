from bs4 import BeautifulSoup


def parse_top_news(html, limit):
    soup = BeautifulSoup(html, 'html.parser')
    for tr in soup.find_all('tr', class_='athing', limit=limit):
        uid = tr.attrs.get('id')
        if not uid:
            continue
        else:
            a = tr.select_one('a.storylink')
            if not a:
                continue
            else:
                link = a.attrs.get('href')
                if not link:
                    continue
                yield uid, link


def parse_comments(html):
    # todo may be in possible span.commtext > a
    soup = BeautifulSoup(html, 'html.parser')
    for span in soup.find_all('span', class_='commtext'):
        for a in span.find_all('a'):
            link = a.attrs.get('href')
            if not link:
                continue
            yield link
