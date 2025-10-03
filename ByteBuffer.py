import os
import struct #un module Python qui permet de convertir des types Python en octets et inversement

PAGE_SIZE = 32  # petite taille pour la démo
FILENAME = "data_pages.bin"


class ByteBuffer:
    def __init__(self, size=PAGE_SIZE):
        self.__bytes = [None] * size
        self.__pos = 0

    def set_position(self, pos):
        self.__pos = pos

    def from_bytes(self, data):
        self.__pos = 0
        self.__bytes = [b for b in data]

    def to_bytes(self):
        return bytes([b if b is not None else 0 for b in self.__bytes])

    # --- INT ---
    def put_int(self, i):
        for j, c in enumerate(i.to_bytes(4, byteorder="big", signed=True)):
            self.__bytes[self.__pos + j] = c
        self.__pos += 4

    def read_int(self):
        b = self.__bytes[self.__pos : self.__pos + 4]
        val = int.from_bytes(b, byteorder="big", signed=True)
        self.__pos += 4
        return val

    # --- FLOAT ---
    def put_float(self, f):
        for j, c in enumerate(struct.pack("<f", f)):  # little-endian float
            self.__bytes[self.__pos + j] = c
        self.__pos += 4

    def read_float(self):
        b = bytes(self.__bytes[self.__pos : self.__pos + 4])
        f = struct.unpack_from("<f", b)[0]
        self.__pos += 4
        return round(f, 2)

    # --- CHAR ---
    def put_char(self, c):
        b = c.encode("utf-8")
        self.__bytes[self.__pos] = b[0]
        self.__pos += 1

    def read_char(self):
        r = self.__bytes[self.__pos]
        r = bytes([r]).decode("utf-8")
        self.__pos += 1
        return r


# ====================== TEST =========================
# 1. Créer un enregistrement avec ByteBuffer
bb = ByteBuffer()
bb.put_int(12345)      # ID
bb.put_float(3.14)     # Salaire
bb.put_char('A')       # Catégorie
record = bb.to_bytes()

# 2. Écrire dans un fichier à la page 0
with open(FILENAME, "wb") as f:  # "wb" pour créer un fichier neuf
    f.seek(0 * PAGE_SIZE)        # aller à la page 0
    f.write(record)

# 3. Lire la page 0
with open(FILENAME, "rb") as f:
    f.seek(0 * PAGE_SIZE)
    page = f.read(PAGE_SIZE)

# 4. Reconstruire l’enregistrement avec ByteBuffer
bb2 = ByteBuffer()
bb2.from_bytes(page)

print("Int:", bb2.read_int())
print("Float:", bb2.read_float())
print("Char:", bb2.read_char())
