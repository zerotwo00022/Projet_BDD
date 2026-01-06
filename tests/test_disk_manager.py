import unittest
import os
import shutil
from dbconfig import DBConfig
from managers.disk_manager import DiskManager


class TestDiskManager(unittest.TestCase):
    TEST_DIR = "./test_db_disk"

    def setUp(self):
        if os.path.exists(self.TEST_DIR): shutil.rmtree(self.TEST_DIR)
        os.makedirs(self.TEST_DIR)
        self.config = DBConfig(self.TEST_DIR, pagesize=4096)

    def tearDown(self):
        if os.path.exists(self.TEST_DIR): shutil.rmtree(self.TEST_DIR)

    def test_alloc_page(self):
        dm = DiskManager(self.config)
        pid = dm.AllocPage()
        self.assertEqual(pid.FileIdx, 0)
        self.assertEqual(pid.PageIdx, 0)
        pid2 = dm.AllocPage()
        self.assertEqual(pid2.PageIdx, 1)

    def test_read_write(self):
        dm = DiskManager(self.config)
        pid = dm.AllocPage()
        
        data = bytearray(self.config.pagesize)
        data[0:5] = b"Hello"
        dm.WritePage(pid, data)
        
        read_buff = bytearray(self.config.pagesize)
        dm.ReadPage(pid, read_buff)
        self.assertEqual(read_buff[0:5], b"Hello")

    def test_persistence(self):
        # 1. On alloue et on libère
        dm = DiskManager(self.config)
        pid = dm.AllocPage()
        dm.DeallocPage(pid)
        dm.Finish() # Sauvegarde free_pages

        # 2. On redémarre
        dm2 = DiskManager(self.config)
        new_pid = dm2.AllocPage()
        # Il doit réutiliser la page 0 car elle était libre
        self.assertEqual(new_pid.FileIdx, 0) 
        self.assertEqual(new_pid.PageIdx, 0)

if __name__ == '__main__':
    unittest.main()