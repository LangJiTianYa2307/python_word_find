import pymysql
import re
from bs4 import BeautifulSoup
from urllib.request import urlopen

conn = pymysql.connect(host='127.0.0.1',user ='root',passwd='1',db='mysql',charset="utf8")

cur = conn.cursor()
cur.execute("USE scraping")
try:
    cur.execute("DROP TABLE python_links")
except:
    print(False)
finally:
    cur.execute("CREATE TABLE python_links(id INT NOT NULL AUTO_INCREMENT,\
                                          fromPageId INT NULL,\
                                          toPageId INT NULL,\
                                          created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,\
                                          PRIMARY KEY(id))")
try:
    cur.execute("DROP TABLE python_pages")
except:
    print(False)
finally:
    cur.execute("CREATE TABLE python_pages(id INT NOT NULL AUTO_INCREMENT,\
                                           url VARCHAR (255) NOT NULL,\
                                           created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\
                                           PRIMARY KEY (id))")

pages = set()
def getLinks(pageUrl,recursionLevel):
    global pages
    print(True)
    if recursionLevel >4:
        return
    pageId = insertPageIfNotExists(pageUrl)
    html_content = urlopen("https://en.wikipedia.org"+pageUrl)
    bsObj = BeautifulSoup(html_content,'html.parser')
    for link in bsObj.find_all('a',href = re.compile("^(/wiki/)((?!:).)*$")):
        insertLink(pageId,insertPageIfNotExists(link.attrs['href']))
        if link.attrs['href'] not in pages:
            newPage = link.attrs['href']
            pages.add(newPage)
            getLinks(newPage,recursionLevel+1)

def insertPageIfNotExists(url):
    cur.execute("SELECT * FROM python_pages WHERE url =%s",(url))
    if cur.rowcount == 0:
        cur.execute("INSERT INTO python_pages (url) VALUES (%s)",(url))
        conn.commit()
        return cur.lastrowid
    else:
        return cur.fetchone()[0]
def insertLink(fromPageId,toPageId):
    cur.execute("SELECT * FROM python_links WHERE fromPageId = %s AND toPageId = %s",(int(fromPageId),int(toPageId)))
    if cur.rowcount == 0:
        cur.execute("INSERT INTO python_links (fromPageId,toPageId) VALUES (%s,%s)",(int(fromPageId),int(toPageId)))
        conn.commit()

# https://en.wikipedia.org
try:
    getLinks("/wiki/Python_(programming_language)",0)
except:
    print(False)
finally:
    cur.close()
    conn.close()
