import json
import os
import csv
from datetime import datetime
import youtube_dl
import traceback
import sqlite3
from selenium import webdriver 
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
import time

class YtDatabase():
    def __init__(self):
        self.__conn = sqlite3.connect("downloadData.db")
        self.__cur = self.__conn.cursor()
        self.__cur.execute(""" CREATE TABLE IF NOT EXISTS Youtuber (
                            id INTEGER PRIMARY KEY,
                            name VARCHAR(500) NOT NULL UNIQUE,
                            subtitle BIT
                    ); """)
        self.__cur.execute(""" CREATE TABLE IF NOT EXISTS Playlist (
                            id INTEGER PRIMARY KEY,
                            yt_id INTEGER,
                            name VARCHAR(500),
                            url VARCHAR(500),
                            refresh BIT
                    ); """)
        self.__cur.execute(""" CREATE TABLE IF NOT EXISTS Url (
                            id INTEGER PRIMARY KEY,
                            play_id INTEGER,
                            url VARCHAR(500) NOT NULL UNIQUE,
                            downloaded BIT
                    ); """)
        self.__conn.commit()

    def add_new(self, allDetails):
        for youtuber in allDetails:
            yt_record = self.check_youtuber(youtuber)
            if yt_record == None:
                yt_record = self.add_youtuber(youtuber, allDetails[youtuber]["subtitle"])
            for playlist in allDetails[youtuber]:
                if playlist == "subtitle":
                    continue
                pl_record = self.check_playlist(playlist, yt_record)
                if pl_record == None:
                    if playlist == "No Playlist":
                        pl_record = self.add_playlist(yt_record, playlist, None, False)
                    else:
                        pl_record = self.add_playlist(yt_record, playlist, allDetails[youtuber][playlist]["url"])
                for url in allDetails[youtuber][playlist]["list"]:
                    try:
                        self.add_url(pl_record, url)
                    except sqlite3.IntegrityError:
                        pass

    def check_youtuber(self, name):
        self.__cur.execute(""" SELECT * FROM Youtuber WHERE name LIKE ? """, (name,))
        row = self.__cur.fetchone()
        if row == None:
            return None
        return row[0]

    def check_playlist(self, name, yt_id):
        self.__cur.execute(""" SELECT * FROM Playlist WHERE name LIKE ? AND yt_id = ? """, (name, yt_id))
        row = self.__cur.fetchone()
        if row == None:
            return None
        return row[0]
    
    def check_url(self, url):
        self.__cur.execute(""" SELECT COUNT(*) FROM Url WHERE url = ? """, (url,))
        return [row for row in self.__cur]

    def add_youtuber(self, name, sub):
        self.__cur.execute(""" INSERT INTO Youtuber(name, subtitle) VALUES (?,?) """, (name, sub))
        self.__conn.commit()
        return self.__cur.lastrowid

    def add_playlist(self, yt_id, name, url, refresh=True):
        self.__cur.execute(""" INSERT INTO Playlist(yt_id, name, url, refresh) VALUES (?,?,?,?) """, (yt_id, name, url, refresh))
        self.__conn.commit()
        return self.__cur.lastrowid

    def add_url(self, pl_id, url):
        self.__cur.execute(""" INSERT INTO Url(play_id, url, downloaded) VALUES (?,?,?) """, (pl_id, url, False))
        self.__conn.commit()

    def playlist_check(self):
        self.__cur.execute(""" SELECT pl.url, yo.subtitle FROM Playlist pl
                                LEFT JOIN Youtuber yo ON pl.yt_id = yo.id
                                WHERE pl.refresh = 1 """)
        return [row for row in self.__cur]

    def fetch_playlists(self):
        self.__cur.execute(""" SELECT pl.id, yo.name, pl.name, yo.subtitle FROM Playlist pl
                            LEFT JOIN Youtuber yo ON pl.yt_id = yo.id """)
        return [row for row in self.__cur]
    
    def fetch_urls(self, pl_id, downloaded = False):
        self.__cur.execute(""" SELECT url FROM Url WHERE downloaded = ? AND play_id = ? """, (downloaded, pl_id))
        return [row for row in self.__cur]

    def to_download(self, randomOrder):
        if randomOrder:
            return self.shuffle_select()
        else:
            self.__cur.execute("""SELECT yo.name, pl.name, yo.subtitle, u.url FROM Url u 
                                LEFT JOIN Playlist pl ON pl.id = u.play_id
                                LEFT JOIN Youtuber yo ON yo.id = pl.yt_id
                                WHERE u.downloaded = 0""")
        return [row for row in self.__cur]
    
    def shuffle_select(self):
        shuffleRows = []
        allCurs = []
        playListDict = {}
        for details in self.fetch_playlists():
            playListDict[details[0]] = details[1:]
            tempCur = self.__conn.cursor()
            tempCur.execute("SELECT play_id, url FROM Url WHERE downloaded = 0 AND play_id = ?", (details[0],))
            allCurs.append(tempCur)

        while allCurs != []:
            deleteIndex = []
            for i in range(len(allCurs)):
                url = allCurs[i].fetchone()
                if url == None:
                    deleteIndex.append(i)
                else:
                    shuffleRows.append([*playListDict[url[0]], url[1]])

            for index in deleteIndex:
                del allCurs[index]

        return shuffleRows

    def mark_downloaded(self, url):
        self.__cur.execute("UPDATE Url SET downloaded = 1 WHERE url = ?", (url,))
        self.__conn.commit()

    def close(self):
        self.__conn.close()

class FileManager():
    def __init__(self):
        self.allDetails = {}
        self.basePath = os.getcwd() + '/'
        with open(self.basePath + 'log.txt', 'w') as log:
            log.write("Last ran at: {0}".format(datetime.now().strftime("%d/%m/%Y, %H:%M:%S")))
        with open(self.basePath + "settings.json", 'r') as settings_json:
            self.settings = json.load(settings_json)

        self.refresh_playlist()
        db.add_new(self.allDetails)
        self.allDetails = {}

        self.find_new()
        db.add_new(self.allDetails)

        self.start_downloads()
        db.close()

    def refresh_playlist(self):
        for row in db.playlist_check():
            self.get_info(row[0], row[1])

    def find_new(self):
        with open('yt_downloads.txt', 'r') as file:
            reader = csv.reader(file, delimiter=',')
            for row in reader:
                if len(row) > 1:
                    if (row[1] == 'page'):
                        pass
                    elif (row[1] == 'playlist'):
                        pass
                    else:
                        self.get_info(row[0], row[1])
                else:
                    self.get_info(row[0])

        with open('yt_downloads.txt', 'w') as _:
            pass

    def getPlayListLinks(self, url, uploader, playlist_name, sub):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)

        driver.get(url)
        actions = ActionChains(driver)

        try:
            element = driver.find_element_by_xpath(self.settings['playListLoadingXPath'])
            while True:
                actions.move_to_element(element).perform()
                time.sleep(10)
                element = driver.find_element_by_xpath(self.settings['playListLoadingXPath'])

        except NoSuchElementException:
            playlist_urls = []
            for a in driver.find_elements_by_xpath(self.settings['playListXPath']):
                playlist_urls.append(a.get_attribute('href').split('&')[0])

            if(uploader in self.allDetails):
                self.allDetails[uploader][playlist_name] = { "list": playlist_urls, "url": url }
            else:
                self.allDetails[uploader] = dict()
                self.allDetails[uploader]['subtitle'] = sub
                self.allDetails[uploader][playlist_name] = { "list": playlist_urls, "url": url }

        driver.quit()

    def get_info(self, url, sub = False):
        ydl = youtube_dl.YoutubeDL({ 'outtmpl': '%(id)s%(ext)s', 'quiet': True, 'ignoreerrors': True })
        with ydl:
            result = ydl.extract_info(url, download=False)
            if result == None:
                return
            if('entries' in result):
                video = result['entries']
                uploader = result['entries'][0]['uploader']
                playlist_name = result['entries'][0]['playlist']
                playlist_urls = []
                for _, item in enumerate(video):
                    try:
                        vid_url = item['webpage_url']
                        playlist_urls.append(vid_url)
                    except TypeError:
                        pass
                if(uploader in self.allDetails):
                    self.allDetails[uploader][playlist_name] = { "list": playlist_urls, "url": url }
                else:
                    self.allDetails[uploader] = dict()
                    self.allDetails[uploader]['subtitle'] = sub
                    self.allDetails[uploader][playlist_name] = { "list": playlist_urls, "url": url }
            else:
                if(result['uploader'] in self.allDetails):
                    if("No Playlist" in self.allDetails[result['uploader']]):
                        self.allDetails[result['uploader']]['No Playlist']["list"].append(result['webpage_url'])
                    else:
                        self.allDetails[result['uploader']]['No Playlist']["list"] = [result['webpage_url']]
                        self.allDetails[result['uploader']]['No Playlist']["url"] = url
                else:
                    self.allDetails[result['uploader']] = { "No Playlist": { "list": [result['webpage_url']], "url": url } }
                    self.allDetails[result['uploader']]['subtitle'] = sub

    def start_downloads(self):
        current = 0
        urlList = db.to_download(self.settings['randomOrder'])
        for url in urlList:
            if(current >= self.settings["downloadLimit"]):
                continue
            options = {
                'outtmpl': self.basePath + 'Youtube/' + url[0] + '/' + url[1] + '/%(title)s.%(ext)s',
                'writesubtitles': url[2],
                'quiet': True
            }
            status = self.download_file(options, url[3])
            self.download_complete(status, url[3])
            if status["Status"]:
                current += 1

    def download_complete(self, status, url):
        if status["Status"]:
            db.mark_downloaded(url)
        else:
            pass

    def download_file(self, options, url):
        with youtube_dl.YoutubeDL(options) as ydl:
            try:
                ydl.download([url])
                return { "Status": True, "Message": "Success" }
            except Exception:
                return { "Status": False, "Message": traceback.format_exc() }

db = YtDatabase()
FileManager()