import unittest
import os
import shutil
from dbconfig import DBConfig
from managers.disk_manager import DiskManager
from managers.buffer_manager import BufferManager
from managers.db_manager import DBManager
from sql.parser import parse
from sql.executor import SQLExecutor


class TestSQLEngine(unittest.TestCase):
    TEST_DIR = "./test_db_sql"

    def setUp(self):
        if os.path.exists(self.TEST_DIR): shutil.rmtree(self.TEST_DIR)
        os.makedirs(self.TEST_DIR)
        self.config = DBConfig(self.TEST_DIR)
        self.disk = DiskManager(self.config)
        self.buff = BufferManager(self.config, self.disk)
        self.db = DBManager(self.config, self.disk, self.buff)
        self.exec = SQLExecutor(self.db)

    def tearDown(self):
        if os.path.exists(self.TEST_DIR): shutil.rmtree(self.TEST_DIR)

    def test_scenario_simple(self):
        # 1. CREATE
        cmd = parse("CREATE TABLE Users (Id:INT, Nom:CHAR(10))")
        self.exec.execute_command(cmd)
        
        # 2. INSERT
        self.exec.execute_command(parse('INSERT INTO Users VALUES (1, "Alice")'))
        self.exec.execute_command(parse('INSERT INTO Users VALUES (2, "Bob")'))
        
        # 3. SELECT avec filtre
        # On vérifie que le parser découpe bien WHERE
        cmd_sel = parse('SELECT * FROM Users WHERE Id=2')
        res = self.exec.execute_command(cmd_sel)
        
        self.assertNotIn("Alice", res)
        self.assertIn("Bob", res)
        self.assertIn("Total selected records=1", res)

    def test_delete(self):
        self.exec.execute_command(parse("CREATE TABLE T (A:INT)"))
        self.exec.execute_command(parse("INSERT INTO T VALUES (100)"))
        
        # Delete
        res = self.exec.execute_command(parse("DELETE FROM T WHERE A=100"))
        self.assertIn("Total deleted records=1", res)
        
        # Vérif vide
        res2 = self.exec.execute_command(parse("SELECT * FROM T"))
        self.assertIn("Total selected records=0", res2)

if __name__ == '__main__':
    unittest.main()