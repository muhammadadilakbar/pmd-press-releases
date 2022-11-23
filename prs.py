"""
This program scrapes the press releases from Pakistan Meteorological Department website.
It stores the press releases in MySQL database. When a new press release is found, the program
sends a notification to email address.
Assumptions: The machine has mail server like Postfix set up.
Tested on: Ubuntu 20.04, Python 3.8.10
"""

from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import re, os
import pymysql
from mysql_connect import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, NOTIFY_EMAIL

def createList( listOfDicts ):
    """
    Takes a list of dictionaries and returns a list containing press release identification numbers
    """
    list = []
    for d in listOfDicts:
        list.append( d['pr_id'] )
    return list

def stripDate( issueDate ):
    """
    Removes the string "Issue Date: " from the beginning
    """
    return issueDate[ 12: ]

def startScraping():
    req = Request( 'https://nwfc.pmd.gov.pk/new/press-releases.php', headers = { 'User-Agent': 'Mozilla/5.0' } )
    html = urlopen( req )
    bs = BeautifulSoup( html, 'html.parser' )
    container = bs.find( "div", { 'class':'contain-wrapp' } )
    pressReleases = container.find_all( 'div', { 'class':'col-md-12', 'id':re.compile('[0-9]+') } )

    for pr in pressReleases:
        prID = int(pr["id"])
        issueDate = stripDate( pr.select("div.col-md-6.pull-right")[0].h5.get_text( " ", strip=True) )
        title = pr.h4.get_text()
        if len( results ) != 0: #the case when there is a new press release
            if prID not in resultsList:
                query = 'INSERT INTO press_releases (pr_id, pr_title, pr_issue_date) VALUES (%s, %s, %s)'
                cursor.execute( query, ( prID, title, issueDate ) )
                emailBody = title + " ISSUED " + issueDate
                commandLine = 'echo "' + emailBody + '" | mail -s "New Press Release" "' + NOTIFY_EMAIL + '"'
                print( "Now sending email" )
                os.system( commandLine )
        else: #the case when database table is empty
            query = 'INSERT INTO press_releases (pr_id, pr_title, pr_issue_date) VALUES (%s, %s, %s)'
            cursor.execute( query, ( prID, title, issueDate ) )


if __name__ == "__main__":
    connection = pymysql.connect( host = DB_HOST, user = DB_USER, passwd = DB_PASSWORD, db = DB_NAME, charset = 'utf8mb4', cursorclass = pymysql.cursors.DictCursor )
    cursor = connection.cursor()
    cursor.execute( "USE " + DB_NAME )
    numberOfRowsReturned = cursor.execute('SELECT pr_id FROM press_releases')

    results = cursor.fetchall()
    resultsList = createList( results )
    startScraping()

    cursor.connection.commit()
    cursor.close()
    connection.close()