from managers.disk_manager import DiskManager
from managers.buffer_manager import BufferManager


if __name__ == "__main__":
    disk = DiskManager("data.bin")
    buffer = BufferManager(disk)

    # On simule une page à écrire
    page0 = buffer.fetch_page(0)
    page0.data[0:11] = b"Hello World"
    buffer.mark_dirty(page0)
    buffer.release_page(page0)

    # Force l’écriture sur disque
    buffer.flush_all()

    # Lecture d’une page existante
    page0b = buffer.fetch_page(0)
    print(page0b.data[:11].decode("utf-8"))  # b'Hello World'  #recuperer la data en binaire et la convertir en chaine de caracteres en decodant avec le code ASCII