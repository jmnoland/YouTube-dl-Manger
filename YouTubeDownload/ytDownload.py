import json
import os
from datetime import datetime
import youtube_dl
import traceback

class FileManager():
    def __init__(self):
        self.__done = {}
        self.__first = {}
        self.basePath = os.getcwd() + '/'
        with open(self.basePath + 'log.txt', 'w') as log:
            log.write("Last ran at: {0}".format(datetime.now().strftime("%d/%m/%Y, %H:%M:%S")))
        self.getUrls()
        self.readFile()

    def getUrls(self):
        allDetails = {}
        with open('yt_downloads.txt', 'r') as file:
            for url in file:
                ydl = youtube_dl.YoutubeDL({ 'outtmpl': '%(id)s%(ext)s', 'quiet': True, 'ignoreerrors': True })
                with ydl:
                    result = ydl.extract_info(url, download=False)
                    if('entries' in result):
                        video = result['entries']
                        uploader = result['entries'][0]['uploader']
                        playlist_name = result['entries'][0]['playlist']
                        playlist_urls = []
                        for i, item in enumerate(video):
                            try:
                                vid_url = item['webpage_url']
                                playlist_urls.append(vid_url)
                            except TypeError:
                                pass
                        if(uploader in allDetails):
                            allDetails[uploader][playlist_name] = playlist_urls
                        else:
                            allDetails[uploader] = dict()
                            allDetails[uploader][playlist_name] = playlist_urls
                    else:
                        if(result['uploader'] in allDetails):
                            if("Other" in allDetails[result['uploader']]):
                                allDetails[result['uploader']]['Other'].append(result['webpage_url'])
                            else:
                                allDetails[result['uploader']]['Other'] = [result['webpage_url']]
                        else:
                            allDetails[result['uploader']] = { "Other": [result['webpage_url']] }
        self.formatJson(allDetails)

    def formatJson(self, newDetails):
        everything = {}
        with open('yt_downloads.json', 'r') as json_file:
            try:
                everything = json.load(json_file)
            except ValueError:
                pass
            for uploader in newDetails:
                if uploader in everything:
                    for title in newDetails[uploader]:
                        if title in everything[uploader]:
                            for url in newDetails[uploader][title]:
                                everything[uploader][title].append(url)
                        else:
                            everything[uploader][title] = newDetails[uploader][title]
                else:
                    everything[uploader] = newDetails[uploader]

        with open('yt_downloads.json', 'w') as afterFormat:
            json.dump(everything, afterFormat, indent=4, sort_keys=True)
        with open('yt_downloads.txt', 'w') as txtFile:
            txtFile.write("")

    def readFile(self):
        with open('yt_downloads.json', 'r') as json_file:
            self.__first = json.load(json_file)
            limit = 0
            for name in self.__first:
                for title in self.__first[name]:
                    index = 1
                    for url in self.__first[name][title]:
                        if(limit >= 5):
                            continue
                        options = {
                            'outtmpl': self.basePath + 'Youtube/' + name + '/' + title + '/%(title)s-%(id)s.%(ext)s',
                            'writesubtitles': True,
                            'quiet': True
                        }
                        self.downloadComplete(self.downloadFile(options, url), name, title, url)
                        index += 1
                        limit += 1
        self.writeFiles()

    def downloadComplete(self, result, name, title, url):
        if(result["Status"]):
            try:
                self.__done[name]
                try:
                    self.__done[name][title]
                except:
                    self.__done[name] = { title: [url]}
                finally:
                    self.__done[name][title].append(url)
            except:
                self.__done[name] = { title: [url] }
        else:
            with open(self.basePath + 'error.txt', 'a') as errorLog:
                errorLog.write("Fail on: " + name + '/' + title + '/' + url + '\nwith error: ' + result['Message'] + "\n\n")

    def writeFiles(self):
        if(self.__done != {}):
            with open('yt_downloads.json', 'w') as json_file:
                for name in self.__first:
                    for title in self.__first[name]:
                        for urlIndex in range(len(self.__first[name][title])):
                            try:
                                self.__done[name]
                                try:
                                    self.__done[name][title]
                                    for doneIndex in range(len(self.__done[name][title])):
                                        if self.__first[name][title][urlIndex] == self.__done[name][title][doneIndex]:
                                            del self.__first[name][title][urlIndex]
                                except:
                                    pass
                            except:
                                pass
                json.dump(self.__first, json_file, indent=4, sort_keys=True)
    
    def downloadFile(self, options, url):
        with youtube_dl.YoutubeDL(options) as ydl:
            try:
                ydl.download([url])
                return { "Status": True, "Message": "Success" }
            except Exception:
                return { "Status": False, "Message": traceback.format_exc() }

FileManager()