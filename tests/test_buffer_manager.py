import unittest
import os
import shutil
from dbconfig import DBConfig
from managers.disk_manager import DiskManager
from managers.buffer_manager import BufferManager

class TestBufferManager(unittest.TestCase):
    TEST_DIR = "./test_db_buffer"

    def setUp(self):
        if os.path.exists(self.TEST_DIR): shutil.rmtree(self.TEST_DIR)
        os.makedirs(self.TEST_DIR)
        # 2 Buffers seulement pour forcer l'éviction LRU
        self.config = DBConfig(self.TEST_DIR, bm_buffercount=2, bm_policy="LRU")
        self.disk = DiskManager(self.config)

    def tearDown(self):
        if os.path.exists(self.TEST_DIR): shutil.rmtree(self.TEST_DIR)

    def test_lru_eviction(self):
        bm = BufferManager(self.config, self.disk)
        
        # Création de 3 pages sur disque
        p0 = self.disk.AllocPage()
        p1 = self.disk.AllocPage()
        p2 = self.disk.AllocPage()

        # Charge P0 et P1 (RAM pleine)
        bm.GetPage(p0)
        bm.FreePage(p0, False) # Pin=0
        
        bm.GetPage(p1)
        bm.FreePage(p1, False) # Pin=0
        
        # P0 est la plus ancienne (LRU).
        # Charge P2 -> P0 doit partir.
        bm.GetPage(p2)
        
        self.assertNotIn(p0, bm.buffer_pool, "P0 aurait dû être évincée par le LRU")
        self.assertIn(p1, bm.buffer_pool)
        self.assertIn(p2, bm.buffer_pool)

    def test_dirty_flag(self):
        bm = BufferManager(self.config, self.disk)
        p0 = self.disk.AllocPage()
        
        # Écriture en RAM uniquement
        buff = bm.GetPage(p0)
        buff[0] = 88 # 'X'
        bm.FreePage(p0, True) # Dirty = True
        
        # Flush vers disque
        bm.FlushBuffers()
        
        # Vérif disque
        check = bytearray(self.config.pagesize)
        self.disk.ReadPage(p0, check)
        self.assertEqual(check[0], 88)

if __name__ == '__main__':
    unittest.main()