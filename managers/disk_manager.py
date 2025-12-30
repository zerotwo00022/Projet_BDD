import os

PAGE_SIZE = 4096

class DiskManager:
    def __init__(self, filename):
        self.filename = filename
        if not os.path.exists(filename):
            open(filename, "wb").close()

    def write_page(self, page_id, data):
        with open(self.filename, "r+b") as f:
            f.seek(page_id * PAGE_SIZE)
            f.write(data)

    def read_page(self, page_id):
        with open(self.filename, "rb") as f:
            f.seek(page_id * PAGE_SIZE)
            return f.read(PAGE_SIZE)
