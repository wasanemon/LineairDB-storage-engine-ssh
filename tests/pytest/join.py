import sys
import mysql.connector
import argparse
from utils.reset import reset

def join_simple(db, cur):
    print("JOIN TEST (SIMPLE)")
    try:
        cur.execute("CREATE DATABASE IF NOT EXISTS ha_lineairdb_test")
        cur.execute("USE ha_lineairdb_test")
        
        # 1. Create tables
        cur.execute("DROP TABLE IF EXISTS child")
        cur.execute("DROP TABLE IF EXISTS parent")
        
        cur.execute("""
            CREATE TABLE parent (
                id INT PRIMARY KEY,
                name VARCHAR(10)
            ) ENGINE=LineairDB
        """)
        
        cur.execute("""
            CREATE TABLE child (
                id INT PRIMARY KEY,
                parent_id INT NOT NULL,
                item VARCHAR(10),
                INDEX idx_parent (parent_id)
            ) ENGINE=LineairDB
        """)

        # 2. Insert data
        cur.execute("INSERT INTO parent VALUES (1, 'A'), (2, 'B')")
        cur.execute("INSERT INTO child VALUES (10, 1, 'apple'), (20, 1, 'banana'), (30, 2, 'cherry')")
        db.commit()

        # 3. Execute JOIN query
        cur.execute("""
            SELECT parent.name, child.item 
            FROM parent 
            JOIN child ON parent.id = child.parent_id
            ORDER BY parent.id, child.id
        """)
        
        results = cur.fetchall()

        # 4. Verify results
        # Expected: ('A', 'apple'), ('A', 'banana'), ('B', 'cherry')
        if len(results) != 3:
            print("\tCheck 1 Failed: Expected 3 rows, got", len(results))
            return 1
        if results[0] != ('A', 'apple'):
            print("\tCheck 2 Failed: Row 1 mismatch")
            return 1
        if results[1] != ('A', 'banana'):
            print("\tCheck 3 Failed: Row 2 mismatch")
            return 1
        if results[2] != ('B', 'cherry'):
            print("\tCheck 4 Failed: Row 3 mismatch")
            return 1
            
        print("\tPassed!")
        return 0
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return 1

def join_composite_index(db, cur):
    print("JOIN TEST (COMPOSITE INDEX)")
    try:
        cur.execute("CREATE DATABASE IF NOT EXISTS ha_lineairdb_test")
        cur.execute("USE ha_lineairdb_test")

        # 1. Create tables
        cur.execute("DROP TABLE IF EXISTS employees")
        cur.execute("DROP TABLE IF EXISTS departments")

        cur.execute("""
            CREATE TABLE departments (
                dept_id INT PRIMARY KEY,
                dept_name VARCHAR(20)
            ) ENGINE=LineairDB
        """)
        
        cur.execute("""
            CREATE TABLE employees (
                id INT PRIMARY KEY,
                dept_id INT NOT NULL,
                role_id INT NOT NULL,
                emp_name VARCHAR(20),
                INDEX idx_dept_role (dept_id, role_id)
            ) ENGINE=LineairDB
        """)

        # 2. Insert data
        cur.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")
        # Dept 1: Role 10 (Alice, Bob), Role 20 (Charlie)
        # Dept 2: Role 30 (Dave)
        cur.execute("INSERT INTO employees VALUES (101, 1, 10, 'Alice')")
        cur.execute("INSERT INTO employees VALUES (102, 1, 10, 'Bob')")
        cur.execute("INSERT INTO employees VALUES (103, 1, 20, 'Charlie')")
        cur.execute("INSERT INTO employees VALUES (201, 2, 30, 'Dave')")
        db.commit()

        # 3. Test Prefix Search (dept_id only)
        cur.execute("""
            SELECT d.dept_name, e.emp_name
            FROM departments d 
            JOIN employees e ON d.dept_id = e.dept_id
            ORDER BY e.id
        """)
        results_prefix = cur.fetchall()
        
        # Expect all 4 employees joined with their departments
        if len(results_prefix) != 4:
            print("\tCheck 1 Failed (Prefix): Expected 4 rows, got", len(results_prefix))
            return 1
        if results_prefix[0] != ('Engineering', 'Alice'):
            print("\tCheck 2 Failed (Prefix): Row 1 mismatch")
            return 1
        if results_prefix[3] != ('Sales', 'Dave'):
            print("\tCheck 3 Failed (Prefix): Row 4 mismatch")
            return 1

        # 4. Test Full Key Search (dept_id AND role_id)
        cur.execute("""
            SELECT d.dept_name, e.emp_name
            FROM departments d 
            JOIN employees e ON d.dept_id = e.dept_id AND e.role_id = 10
            ORDER BY e.id
        """)
        results_full = cur.fetchall()

        # Expect only Alice and Bob
        if len(results_full) != 2:
            print("\tCheck 4 Failed (Full): Expected 2 rows, got", len(results_full))
            return 1
        if results_full[0] != ('Engineering', 'Alice'):
            print("\tCheck 5 Failed (Full): Row 1 mismatch")
            return 1
        if results_full[1] != ('Engineering', 'Bob'):
            print("\tCheck 6 Failed (Full): Row 2 mismatch")
            return 1

        print("\tPassed!")
        return 0

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return 1

def main():
    # Manual execution entry point
    db = mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor = db.cursor()
    
    if join_simple(db, cursor) != 0:
        sys.exit(1)
    if join_composite_index(db, cursor) != 0:
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to MySQL')
    parser.add_argument('--user', metavar='user', type=str,
                        help='name of user',
                        default="root")
    parser.add_argument('--password', metavar='pw', type=str,
                        help='password for the user',
                        default="")
    args = parser.parse_args()
    main()
