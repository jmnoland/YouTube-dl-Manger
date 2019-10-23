import json
from datetime import datetime
import youtube_dl
import traceback

class FileManager():
    def __init__(self):
        self.__done = {}
        self.__first = {}
        self.basePath = ""
        with open(self.basePath + 'log.txt', 'w') as log:
            log.write("Last ran at: {0}".format(datetime.now().strftime("%d/%m/%Y, %H:%M:%S")))
        self.readFile()

    def readFile(self):
        with open('yt_downloads.json', 'r') as json_file:
            self.__first = json.load(json_file)
            for name in self.__first:
                for title in self.__first[name]:
                    index = 1
                    for url in self.__first[name][title]:
                        options = { 
                            'outtmpl': self.basePath + 'Youtube/' + name + '/' + title + '/%(title)s-%(id)s.%(ext)s', 
                            'writesubtitles': True
                            }
                        if("playlist" in url):
                            pass
                        else:
                            pass
                            self.downloadComplete(self.downloadFile(options, url), name, title, url)
                        index += 1
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
            print(result)

    def writeFiles(self):
        if(self.__done != {}):
            with open('yt_downloads.json', 'w') as json_file:
                for name in self.__first:
                    for title in self.__first[name]:
                        for url in self.__first[name][title]:
                            try:
                                self.__done[name]
                                try:
                                    self.__done[name][title]
                                    if url in self.__done[name][title]:
                                        self.__first[name][title].remove(url)
                                except:
                                    pass
                            except:
                                pass
                json.dump(self.__first, json_file)

            with open('yt_downloads_done.json', 'w') as json_file:
                json.dump(self.__done, json_file)
    
    def downloadFile(self, options, url):
        with youtube_dl.YoutubeDL(options) as ydl:
            try:
                ydl.download([url])
                return { "Status": True, "Message": "Success" }
            except Exception:
                return { "Status": False, "Message": traceback.format_exc() }

FileManager()