#!/usr/bin/env python3
# run_sgbd.py
"""
Interactive SGBD CLI - Execute SQL commands interactively
Run with: python run_sgbd.py
"""
import os
import sys
import json
from Projet_BDD.dbconfig import DBConfig
from Projet_BDD.managers.disk_manager import DiskManager
from Projet_BDD.managers.buffer_manager import BufferManager
from Projet_BDD.managers.dbManager import DBManager
from Projet_BDD.relation import Relation
from Projet_BDD.record import Record

class SGBDCLI:
    def __init__(self):
        self.config = None
        self.disk = None
        self.buffer = None
        self.dbm = None
        self.connected_db = None
        self.current_path = None
    
    def show_help(self):
        """Display help information"""
        help_text = """
╔════════════════════════════════════════════════════════════════╗
║          Interactive SGBD - SQL Command Reference              ║
╚════════════════════════════════════════════════════════════════╝

DATABASE COMMANDS:
  CREATE DATABASE <name>              Create a new database
  USE <database_name>                 Connect to a database
  DROP DATABASE <name>                Drop a database
  SHOW DATABASES                      List all databases

TABLE COMMANDS:
  CREATE TABLE <name> (col1:TYPE, col2:TYPE, ...)
                                      Create a table
  DROP TABLE <name>                   Drop a table
  DESCRIBE <table_name>               Show table structure
  SHOW TABLES                         List all tables

DATA COMMANDS:
  INSERT INTO <table> VALUES (val1, val2, ...)
                                      Insert a record
  SELECT * FROM <table>               Select all records
  SELECT * FROM <table> WHERE cond    Select with condition
  DELETE FROM <table>                 Delete all records

UTILITY COMMANDS:
  HELP                                Show this help
  STATUS                              Show current status
  EXIT                                Close database and exit

EXAMPLES:
  CREATE DATABASE mydb
  USE mydb
  CREATE TABLE users (id:INT, name:CHAR(20), age:INT)
  INSERT INTO users VALUES (1, 'Alice', 25)
  INSERT INTO users VALUES (2, 'Bob', 30)
  SELECT * FROM users
  SELECT * FROM users WHERE age > 25
  SELECT * FROM users WHERE name = "Alice"
  DESCRIBE users
  DROP TABLE users
  EXIT
"""
        print(help_text)
    
    def show_status(self):
        """Show current connection status"""
        if self.connected_db:
            print(f"\n✓ Connected to database: {self.connected_db}")
            print(f"  Path: {self.current_path}")
            if self.dbm:
                tables = self.dbm.tables
                print(f"  Tables: {len(tables)}")
                if tables:
                    for name in tables:
                        print(f"    - {name}")
        else:
            print("\n✗ Not connected to any database")
        print()
    
    def create_database(self, db_name):
        """CREATE DATABASE db_name"""
        try:
            path = f"./databases/{db_name}"
            os.makedirs(path, exist_ok=True)
            
            # Create config
            config_path = os.path.join(path, "config.json")
            config_data = {
                "dbpath": path,
                "pagesize": 4096,
                "bm_buffercount": 10,
                "bm_policy": "LRU"
            }
            with open(config_path, "w") as f:
                json.dump(config_data, f, indent=2)
            
            print(f"✓ Database '{db_name}' created")
            return True
        except Exception as e:
            print(f"✗ Error: {e}")
            return False
    
    def use_database(self, db_name):
        """USE database_name"""
        try:
            path = f"./databases/{db_name}"
            if not os.path.exists(path):
                print(f"✗ Database '{db_name}' does not exist")
                return False
            
            # Close previous connection
            if self.disk:
                if self.dbm:
                    self.dbm.SaveState()
                if self.buffer:
                    self.buffer.FlushBuffers()
                self.disk.Finish()
            
            # Connect to new database
            config_path = os.path.join(path, "config.json")
            if not os.path.exists(config_path):
                print(f"✗ Database config not found")
                return False
            
            self.config = DBConfig.LoadDBConfig(config_path)
            self.disk = DiskManager(self.config)
            self.disk.Init()
            self.buffer = BufferManager(self.config, self.disk)
            self.dbm = DBManager(self.config, self.disk, self.buffer)
            self.dbm.LoadState()
            
            self.connected_db = db_name
            self.current_path = path
            print(f"✓ Connected to database '{db_name}'")
            return True
        except Exception as e:
            print(f"✗ Error: {e}")
            return False
    
    def show_databases(self):
        """SHOW DATABASES"""
        try:
            db_path = "./databases"
            if not os.path.exists(db_path):
                print("(No databases)")
                return True
            
            dbs = [d for d in os.listdir(db_path) if os.path.isdir(os.path.join(db_path, d))]
            if not dbs:
                print("(No databases)")
                return True
            
            print("\nDatabases:")
            for db in sorted(dbs):
                marker = "→" if db == self.connected_db else " "
                print(f"  {marker} {db}")
            print()
            return True
        except Exception as e:
            print(f"✗ Error: {e}")
            return False
    
    def drop_database(self, db_name):
        """DROP DATABASE db_name"""
        try:
            # If it's the connected database, close the connection first
            if db_name == self.connected_db:
                self.close()
                self.connected_db = None
                self.current_path = None
            
            path = f"./databases/{db_name}"
            if not os.path.exists(path):
                print(f"✗ Database '{db_name}' does not exist")
                return False
            
            # Remove all files in the database directory
            import shutil
            shutil.rmtree(path)
            print(f"✓ Database '{db_name}' dropped")
            return True
        except Exception as e:
            print(f"✗ Error: {e}")
            return False
    
    def create_table(self, table_name, columns_str):
        """CREATE TABLE table_name (col1:TYPE1, col2:TYPE2, ...)
        Also supports: CREATE TABLE table_name (col1 TYPE1, col2 TYPE2, ...)
        """
        if not self.connected_db:
            print("✗ No database connected. Use: USE <database_name>")
            return False
        
        try:
            col_names = []
            col_types = []
            
            # Split by comma to get individual columns
            for part in columns_str.split(","):
                part = part.strip()
                
                # Try two formats: "name:TYPE" or "name TYPE"
                if ":" in part:
                    # Format: col1:INT
                    cname, ctype = part.split(":", 1)
                    col_names.append(cname.strip())
                    col_types.append(ctype.strip())
                else:
                    # Format: col1 INT or col1 CHAR(50)
                    tokens = part.split(None, 1)
                    if len(tokens) == 2:
                        col_names.append(tokens[0].strip())
                        col_types.append(tokens[1].strip())
                    else:
                        print(f"✗ Invalid column format: {part}")
                        print("   Use: col_name:TYPE or col_name TYPE")
                        return False
            
            relation = Relation(table_name, col_names, col_types, self.disk, self.buffer)
            self.dbm.AddTable(relation)
            print(f"✓ Table '{table_name}' created")
            return True
        except Exception as e:
            print(f"✗ Error: {e}")
            return False
    
    def drop_table(self, table_name):
        """DROP TABLE table_name"""
        if not self.connected_db:
            print("✗ No database connected")
            return False
        
        try:
            self.dbm.RemoveTable(table_name)
            print(f"✓ Table '{table_name}' dropped")
            return True
        except Exception as e:
            print(f"✗ Error: {e}")
            return False
    
    def insert_record(self, table_name, values_str):
        """INSERT INTO table_name VALUES (val1, val2, ...)
        Supports single or multiple rows
        """
        if not self.connected_db:
            print("✗ No database connected")
            return False
        
        try:
            table = self.dbm.GetTable(table_name)
            if not table:
                print(f"✗ Table '{table_name}' not found")
                return False
            
            # Remove semicolon if present
            values_str = values_str.rstrip(";").strip()
            
            # Handle multiple rows: VALUES (...), (...)
            row_count = 0
            rows = []
            
            # Find all parenthesized groups
            i = 0
            while i < len(values_str):
                if values_str[i] == "(":
                    # Find matching closing parenthesis
                    depth = 1
                    j = i + 1
                    while j < len(values_str) and depth > 0:
                        if values_str[j] == "(":
                            depth += 1
                        elif values_str[j] == ")":
                            depth -= 1
                        j += 1
                    
                    # Extract the row
                    row_content = values_str[i+1:j-1].strip()
                    rows.append(row_content)
                    i = j
                    
                    # Skip comma and whitespace
                    while i < len(values_str) and values_str[i] in ",\t\n ":
                        i += 1
                else:
                    i += 1
            
            if not rows:
                print("✗ No valid rows found in VALUES clause")
                return False
            
            # Insert each row
            for row_content in rows:
                # Parse values - split by comma but respect quoted strings
                values = []
                current = ""
                in_quotes = False
                
                for char in row_content:
                    if char == '"' or char == "'":
                        in_quotes = not in_quotes
                        current += char
                    elif char == "," and not in_quotes:
                        values.append(current.strip())
                        current = ""
                    else:
                        current += char
                
                if current:
                    values.append(current.strip())
                
                if len(values) != len(table.columns):
                    print(f"✗ Column count mismatch: expected {len(table.columns)}, got {len(values)}")
                    return False
                
                # Convert to proper types
                converted = []
                for i, val in enumerate(values):
                    col_type = table.columns[i].base_type
                    
                    # Remove quotes
                    if (val.startswith('"') and val.endswith('"')) or \
                       (val.startswith("'") and val.endswith("'")):
                        val = val[1:-1]
                    
                    # Type conversion
                    if col_type == "INT":
                        try:
                            converted.append(int(val))
                        except ValueError:
                            print(f"✗ Invalid INT value: {val}")
                            return False
                    elif col_type == "FLOAT":
                        try:
                            converted.append(float(val))
                        except ValueError:
                            print(f"✗ Invalid FLOAT value: {val}")
                            return False
                    else:
                        # CHAR or VARCHAR
                        converted.append(val)
                
                # Insert the record
                record = Record(converted)
                table.InsertRecord(record)
                row_count += 1
            
            if row_count == 1:
                print(f"✓ 1 record inserted")
            else:
                print(f"✓ {row_count} records inserted")
            return True
        except Exception as e:
            print(f"✗ Error: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def evaluate_where(self, record, table, where_clause):
        """Evaluate WHERE clause against a record.
        Supports: col = value, col > value, col < value, col >= value, col <= value, col != value
        Supports AND, OR operators
        """
        try:
            # Replace logical operators with Python equivalents
            condition = where_clause
            
            # Split by AND/OR while preserving structure
            # For simplicity, evaluate conditions sequentially
            
            # Handle multiple conditions with AND
            if " AND " in condition.upper():
                parts = condition.split(" AND ")
                for part in parts:
                    if not self.evaluate_single_condition(record, table, part.strip()):
                        return False
                return True
            
            # Handle multiple conditions with OR
            elif " OR " in condition.upper():
                parts = condition.split(" OR ")
                for part in parts:
                    if self.evaluate_single_condition(record, table, part.strip()):
                        return True
                return False
            
            else:
                return self.evaluate_single_condition(record, table, condition.strip())
        
        except Exception as e:
            print(f"✗ WHERE evaluation error: {e}")
            return False
    
    def evaluate_single_condition(self, record, table, condition):
        """Evaluate a single condition like: col = value"""
        try:
            # Find comparison operator
            operators = [">=", "<=", "!=", "=", ">", "<"]
            op = None
            op_pos = -1
            
            for o in operators:
                pos = condition.find(o)
                if pos != -1:
                    op = o
                    op_pos = pos
                    break
            
            if op is None or op_pos == -1:
                return False
            
            col_name = condition[:op_pos].strip()
            value_str = condition[op_pos + len(op):].strip()
            
            # Find column index
            col_idx = -1
            for i, col in enumerate(table.columns):
                if col.name.lower() == col_name.lower():
                    col_idx = i
                    break
            
            if col_idx == -1:
                print(f"✗ Column '{col_name}' not found")
                return False
            
            # Get record value
            rec_val = record.values[col_idx]
            
            # Parse value (remove quotes)
            if (value_str.startswith('"') and value_str.endswith('"')) or \
               (value_str.startswith("'") and value_str.endswith("'")):
                value_str = value_str[1:-1]
            
            # Convert to proper type
            col_type = table.columns[col_idx].base_type
            if col_type == "INT":
                try:
                    value = int(value_str)
                    rec_val = int(rec_val)
                except:
                    value = value_str
            elif col_type == "FLOAT":
                try:
                    value = float(value_str)
                    rec_val = float(rec_val)
                except:
                    value = value_str
            else:
                value = value_str
                # For string comparison, normalize
                rec_val = str(rec_val).strip()
            
            # Evaluate condition
            if op == "=":
                return rec_val == value
            elif op == "!=":
                return rec_val != value
            elif op == ">":
                return rec_val > value
            elif op == "<":
                return rec_val < value
            elif op == ">=":
                return rec_val >= value
            elif op == "<=":
                return rec_val <= value
            
            return False
        except Exception as e:
            print(f"✗ Condition evaluation error: {e}")
            return False
    
    def select_all(self, table_name, where_clause=None):
        """SELECT * FROM table_name [WHERE condition]"""
        if not self.connected_db:
            print("✗ No database connected")
            return False
        
        try:
            table = self.dbm.GetTable(table_name)
            if not table:
                print(f"✗ Table '{table_name}' not found")
                return False
            
            all_records = table.GetAllRecords()
            
            # Filter by WHERE clause if provided
            if where_clause:
                records = []
                for rec in all_records:
                    if self.evaluate_where(rec, table, where_clause):
                        records.append(rec)
            else:
                records = all_records
            
            # Print header with column widths
            col_widths = []
            for col in table.columns:
                width = max(len(col.name), 12)
                col_widths.append(width)
            
            header = " | ".join([f"{col.name:<{col_widths[i]}}" 
                                for i, col in enumerate(table.columns)])
            print("\n" + header)
            print("-" * len(header))
            
            if not records:
                print("(0 rows)")
                return True
            
            # Print records
            for rec in records:
                row = []
                for i, val in enumerate(rec.values):
                    col = table.columns[i]
                    val_str = str(val)
                    
                    # Remove extra quotes/parens if present
                    if val_str.startswith('("') and val_str.endswith('");'):
                        val_str = val_str[2:-3]  # Remove (" and ");
                    elif val_str.startswith('"') and val_str.endswith('";'):
                        val_str = val_str[1:-2]  # Remove " and ";
                    
                    if col.base_type in ("CHAR", "VARCHAR"):
                        row.append(f"{val_str:<{col_widths[i]}}")
                    elif col.base_type == "FLOAT":
                        try:
                            row.append(f"{float(val_str):<{col_widths[i]}.2f}")
                        except:
                            row.append(f"{val_str:<{col_widths[i]}}")
                    else:
                        row.append(f"{val_str:<{col_widths[i]}}")
                
                print(" | ".join(row))
            
            print(f"\n({len(records)} rows)")
            return True
        except Exception as e:
            print(f"✗ Error: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def describe_table(self, table_name):
        """DESCRIBE table_name"""
        if not self.connected_db:
            print("✗ No database connected")
            return False
        
        try:
            table = self.dbm.GetTable(table_name)
            if not table:
                print(f"✗ Table '{table_name}' not found")
                return False
            
            print(f"\nTable: {table_name}")
            print("-" * 50)
            for col in table.columns:
                print(f"  {col.name:18} {col.type_str:15} Size: {col.fixed_size:4} bytes")
            print()
            return True
        except Exception as e:
            print(f"✗ Error: {e}")
            return False
    
    def show_tables(self):
        """SHOW TABLES"""
        if not self.connected_db:
            print("✗ No database connected")
            return False
        
        try:
            tables = self.dbm.tables
            if not tables:
                print("(No tables)")
                return True
            
            print("\nTables:")
            for name in sorted(tables.keys()):
                table = tables[name]
                cols = ", ".join([c.name for c in table.columns])
                print(f"  {name:20} ({cols})")
            print()
            return True
        except Exception as e:
            print(f"✗ Error: {e}")
            return False
    
    def delete_all(self, table_name):
        """DELETE FROM table_name"""
        if not self.connected_db:
            print("✗ No database connected")
            return False
        
        try:
            table = self.dbm.GetTable(table_name)
            if not table:
                print(f"✗ Table '{table_name}' not found")
                return False
            
            records = table.GetAllRecords()
            # Note: In a real implementation, we'd track RecordIds
            # For now, we show that DELETE is supported
            print(f"✗ DELETE all not fully supported (need to track RecordIds)")
            return False
        except Exception as e:
            print(f"✗ Error: {e}")
            return False
    
    def close(self):
        """Close connection"""
        try:
            if self.disk:
                if self.dbm:
                    self.dbm.SaveState()
                if self.buffer:
                    self.buffer.FlushBuffers()
                self.disk.Finish()
            self.connected_db = None
            print("✓ Database closed")
        except Exception as e:
            print(f"✗ Error: {e}")
    
    def run(self):
        """Main CLI loop"""
        print("\n" + "=" * 70)
        print("  Interactive SGBD - SQL Command Line Interface")
        print("=" * 70)
        print("  Type 'HELP' for commands or 'EXIT' to quit\n")
        
        while True:
            try:
                db_indicator = f" [{self.connected_db}]" if self.connected_db else ""
                prompt = f"SGBD{db_indicator}> "
                user_input = input(prompt).strip()
                
                if not user_input:
                    continue
                
                upper_input = user_input.upper()
                
                # Parse commands
                if upper_input == "HELP":
                    self.show_help()
                
                elif upper_input == "STATUS":
                    self.show_status()
                
                elif upper_input.startswith("CREATE DATABASE"):
                    parts = user_input.split(None, 2)
                    if len(parts) >= 3:
                        db_name = parts[2].strip()
                        self.create_database(db_name)
                    else:
                        print("✗ Syntax: CREATE DATABASE <name>")
                
                elif upper_input.startswith("USE "):
                    db_name = user_input[4:].strip()
                    self.use_database(db_name)
                
                elif upper_input == "SHOW DATABASES":
                    self.show_databases()
                
                elif upper_input.startswith("DROP DATABASE"):
                    db_name = user_input[13:].strip()
                    self.drop_database(db_name)
                
                elif upper_input.startswith("CREATE TABLE"):
                    parts = user_input.split(None, 2)
                    if len(parts) >= 3 and "(" in user_input:
                        rest = parts[2]
                        table_name = rest[:rest.find("(")].strip()
                        cols_str = rest[rest.find("(")+1:rest.rfind(")")].strip()
                        self.create_table(table_name, cols_str)
                    else:
                        print("✗ Syntax: CREATE TABLE <name> (col1:TYPE, col2:TYPE, ...)")
                
                elif upper_input.startswith("DROP TABLE"):
                    table_name = user_input[10:].strip()
                    self.drop_table(table_name)
                
                elif upper_input.startswith("DESCRIBE"):
                    table_name = user_input[8:].strip()
                    self.describe_table(table_name)
                
                elif upper_input == "SHOW TABLES":
                    self.show_tables()
                
                elif upper_input.startswith("INSERT INTO"):
                    if "VALUES" in upper_input:
                        parts = upper_input.split("VALUES", 1)
                        table_part = parts[0].replace("INSERT INTO", "").strip()
                        # Get original case values
                        orig_parts = user_input.split("VALUES", 1)
                        values_part = orig_parts[1].strip()
                        self.insert_record(table_part, values_part)
                    else:
                        print("✗ Syntax: INSERT INTO <table> VALUES (val1, val2, ...)")
                
                elif upper_input.startswith("SELECT * FROM"):
                    # Handle SELECT with WHERE clause
                    # Split by WHERE first
                    upper_for_split = upper_input.upper()
                    
                    if " WHERE " in upper_for_split:
                        # Find WHERE position in uppercase version
                        where_idx = upper_for_split.find(" WHERE ")
                        # Extract from original using same position
                        table_name = user_input[14:where_idx].strip()
                        where_clause = user_input[where_idx + 7:].strip()  # 7 = len(" WHERE ")
                        self.select_all(table_name, where_clause)
                    else:
                        table_name = user_input[14:].strip()
                        self.select_all(table_name)
                
                elif upper_input.startswith("DELETE FROM"):
                    table_name = user_input[11:].strip()
                    self.delete_all(table_name)
                
                elif upper_input == "EXIT":
                    self.close()
                    print("Goodbye!")
                    break
                
                else:
                    print(f"✗ Unknown command: '{user_input}'")
                    print("  Type 'HELP' for available commands")
                
                print()
            
            except KeyboardInterrupt:
                print("\n")
                self.close()
                print("Interrupted")
                break
            except EOFError:
                self.close()
                break
            except Exception as e:
                print(f"✗ Error: {e}")


if __name__ == "__main__":
    cli = SGBDCLI()
    cli.run()
