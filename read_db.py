import sqlite3

class YtDatabase():
    def __init__(self):
        self.__conn = sqlite3.connect("downloadData.db")
        self.__cur = self.__conn.cursor()
        self.read_youtuber()
        print("")
        self.read_playlist()
        print("")
        self.read_urls()
        print("")
        self.read_err()
        self.__conn.close()

    def read_youtuber(self):
        self.__cur.execute("SELECT * FROM Youtuber")
        for row in self.__cur:
            print(row)

    def read_playlist(self):
        self.__cur.execute("SELECT * FROM Playlist WHERE refresh = 1")
        for row in self.__cur:
            print(row)

    def read_urls(self):
        self.__cur.execute("SELECT * FROM Url WHERE downloaded = 0")
        for row in self.__cur:
            print(row)

    def read_err(self):
        self.__cur.execute("SELECT * FROM Error")
        for row in self.__cur:
            print(row)

    def clear_all(self):
        self.__cur.execute("DELETE FROM Url")
        self.__conn.commit()

YtDatabase()