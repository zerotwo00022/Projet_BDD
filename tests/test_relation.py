import unittest
import os
import shutil
from dbconfig import DBConfig
from managers.disk_manager import DiskManager
from managers.buffer_manager import BufferManager
from managers.relation import Relation, Record

class TestRelation(unittest.TestCase):
    TEST_DIR = "./test_db_rel"

    def setUp(self):
        if os.path.exists(self.TEST_DIR): shutil.rmtree(self.TEST_DIR)
        os.makedirs(self.TEST_DIR)
        self.config = DBConfig(self.TEST_DIR)
        self.disk = DiskManager(self.config)
        self.buff = BufferManager(self.config, self.disk)

    def tearDown(self):
        if os.path.exists(self.TEST_DIR): shutil.rmtree(self.TEST_DIR)

    def test_insert_select_binary(self):
        schema = [("Id", "INT"), ("Prix", "FLOAT"), ("Nom", "CHAR(10)")]
        rel = Relation("Produits", schema, self.disk, self.buff)
        rel.add_data_page() # On initialise au moins une page
        
        # Test Float et String
        rec = Record([1, 99.99, "SuperProd"])
        rid = rel.InsertRecord(rec)
        
        # Force l'écriture
        self.buff.FlushBuffers()
        
        # Relecture
        read_rec = rel.read_record_from_page(rid.page_id, rid.slot_idx)
        
        self.assertEqual(read_rec.values[0], 1)
        self.assertAlmostEqual(read_rec.values[1], 99.99, places=2)
        self.assertEqual(read_rec.values[2], "SuperProd")

    def test_update_record(self):
        rel = Relation("Test", [("Val", "INT")], self.disk, self.buff)
        rel.add_data_page()
        
        rid = rel.InsertRecord(Record([10]))
        # Lecture du record pour avoir le RID attaché
        rec = rel.read_record_from_page(rid.page_id, rid.slot_idx)
        rec.rid = rid
        
        # Update
        rel.UpdateRecord(rec, [20])
        
        # Vérif
        check = rel.read_record_from_page(rid.page_id, rid.slot_idx)
        self.assertEqual(check.values[0], 20)

if __name__ == '__main__':
    unittest.main()