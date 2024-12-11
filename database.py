import psycopg2
import datetime
from datetime import datetime, timedelta
import random

DB_CONFIG = {
    'dbname': 'School',
    'user': 'gaussdb',
    'password': 'Enmo@123',
    'host': '172.27.12.99',
    'port': '5432'
}

def create_tables():
    """创建所需的数据库表"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        #创建主节点表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Main_ip (
                child_ip_address varchar(20) NOT NULL,
                PRIMARY KEY (child_ip_address)
            );
        ''')
        
        #子节点
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Child_ip (
                current_ip_address varchar(20) NOT NULL,
                PRIMARY KEY (current_ip_address),
                category varchar(20),
                cpu_number int4,
                rom_number int4,
                CONSTRAINT child_ip_one FOREIGN KEY (current_ip_address) REFERENCES Main_ip(child_ip_address)
                
            );
        ''')

        
        # 创建SystemContainer表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS System_Container (
                category varchar(20) NOT NULL,
                PRIMARY KEY (category),
                category_name varchar(20),
                status VARCHAR(50) CHECK (status IN ('active', 'unactive'))

            );
            
        ''')

        # 创建Language_Container表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Language_Container (
                category varchar(20) NOT NULL,
                PRIMARY KEY (category),
                category_name varchar(20),
                status VARCHAR(50) CHECK (status IN ('active', 'unactive'))

            );
            
        ''')

        # 创建Work_Container表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Work_Container (
                category varchar(20) NOT NULL,
                PRIMARY KEY (category),
                category_name varchar(20),
                status VARCHAR(50) CHECK (status IN ('active', 'unactive'))

            );
            
        ''')

        #创建Task表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Task (
                Task_id varchar(20) NOT NULL,
                PRIMARY KEY (Task_id),
                cpu_number int4,
                rom_number int4,
                status VARCHAR(50) CHECK (status IN ('failed','queued','completed','running'))

            );
            
        ''')

        #创建Task_System表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Task_System (
                Task_id varchar(20) NOT NULL,
                category_name varchar(20),
                CONSTRAINT Task_System1 FOREIGN KEY (Task_id) REFERENCES Task(Task_id)
            );
        ''')

        #创建Task_Language表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Task_Language (
                Task_id varchar(20) NOT NULL,
                category_name varchar(20),
                CONSTRAINT Task_Language1 FOREIGN KEY (Task_id) REFERENCES Task(Task_id)
            );
        ''')

        #创建Task_Work表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Task_Work (
                Task_id varchar(20) NOT NULL,
                category_name varchar(20),
                CONSTRAINT Task_Work1 FOREIGN KEY (Task_id) REFERENCES Task(Task_id)
            );
        ''')


        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_tables()
