import chromadb
import os
import shutil
import databutton as db


class Chroma:
    path = ".tmp/chroma/"
    file_path = "path/chroma.sqlite3"

    def __init__(self):
        pass

    def persist(self):
        shutil.make_archive("chroma", "zip", self.path)
        with open("chroma.zip", "rb") as file:
            db.storage.binary.put("chromadb", file.read())

    def client(self):
        # if local file doesn't exists, try to fetch it
        if not os.path.exists(self.file_path):
            try:
                stream = db.storage.binary.get("chromadb")
                with open(".tmp/chroma/chroma.zip", "wb") as file:
                    file.write(stream)
                shutil.unpack_archive("chroma.zip", self.path)
            except FileNotFoundError:
                pass
        # Instigate the client
        the_client = chromadb.PersistentClient(self.path)
        the_client.persist = self.persist

        return the_client
