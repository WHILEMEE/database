from ipaddress import ip_address
from random import randint

import psycopg2
import datetime
from datetime import datetime, timedelta
import random

from unicodedata import category

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
        
        #子节点A
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ChildA_ip (
                current_ip_address varchar(20) NOT NULL,
                PRIMARY KEY (current_ip_address),
                category varchar(20),
                cpu_number int4,
                memory int4      
            );
        ''')

        # 子节点B
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ChildB_ip (
                current_ip_address varchar(20) NOT NULL,
                PRIMARY KEY (current_ip_address),
                category varchar(20),
                cpu_number int4,
                memory int4
            );
        ''')

        # 子节点C
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ChildC_ip (
                current_ip_address varchar(20) NOT NULL,
                PRIMARY KEY (current_ip_address),
                category varchar(20),
                cpu_number int4,
                memory int4
            );
        ''')

        # 子节点D
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ChildD_ip (
                current_ip_address varchar(20) NOT NULL,
                PRIMARY KEY (current_ip_address),
                category varchar(20),
                cpu_number int4,
                memory int4
            );
        ''')

        # 子节点E
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ChildE_ip (
                current_ip_address varchar(20) NOT NULL,
                PRIMARY KEY (current_ip_address),
                category varchar(20),
                cpu_number int4,
                memory int4
            );
        ''')

        # 子节点F
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ChildF_ip (
                current_ip_address varchar(20) NOT NULL,
                PRIMARY KEY (current_ip_address),
                category varchar(20),
                cpu_number int4,
                memory int4
            );
        ''')



        #主-子节点
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Main_Child (
                main_child_ip_address varchar(20) NOT NULL,
                child_name varchar(20)
            );
        ''')

        
        # 创建SystemContainer表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS System_Container (
                category varchar(20) NOT NULL,
                category_name varchar(20),
                PRIMARY KEY (category_name),
                status VARCHAR(50) CHECK (status IN ('active', 'unactive'))
            );       
        ''')

        # 创建Language_Container表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Language_Container (
                category varchar(20) NOT NULL,
                category_name varchar(20),
                PRIMARY KEY (category_name),
                status VARCHAR(50) CHECK (status IN ('active', 'unactive'))
            );           
        ''')

        # 创建Work_Container表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Work_Container (
                category varchar(20) NOT NULL,
                category_name varchar(20),
                PRIMARY KEY (category_name),
                status VARCHAR(50) CHECK (status IN ('active', 'unactive'))
            ); 
        ''')

        #创建Task表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Task (
                Task_id varchar(20) NOT NULL,
                PRIMARY KEY (Task_id),
                cpu_number int4,
                memory int4,
                status VARCHAR(50) CHECK (status IN ('failed','queued','completed','running'))
            );
            
        ''')

        #创建Task_System表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Task_System (
                Task_id varchar(20) NOT NULL,
                category_name varchar(20)
            );
        ''')

        #创建Task_Language表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Task_Language (
                Task_id varchar(20) NOT NULL,
                category_name varchar(20)
            );
        ''')

        #创建Task_Work表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS Task_Work (
                Task_id varchar(20) NOT NULL,
                category_name varchar(20)
            );
        ''')


        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_data():
    """模拟虚拟机集群的行为"""
    child_names = ['ChildA_ip', 'ChildB_ip', 'ChildC_ip', 'ChildD_ip', 'ChildE_ip', 'ChildF_ip']
    ip_addresses = ['111.111.111.1','111.111.111.2','111.111.111.3','111.111.111.4','111.111.111.5','111.111.111.6']
    system_names = ['Windows','MacOS','Unix','Linux','DOS']
    language_names = ['Java','Python','C++','C#','JavaScript']
    work_names = ['MySQL','PostgreSQL','Oracle','Sybase','Clipper']
    container_statuses = ['active', 'unactive']
    task_statuses = ['failed','queued','completed','running']
    categorys = ['System','Language','Work']


    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
#每个容器池表放入20个数据

        for i in range(0, 5):
            category = f'System'
            category_name = system_names[i]
            status = random.choice(container_statuses)

            # 尝试插入新记录
            # 先是系统类容器池表
            cur.execute('''
                INSERT INTO System_Container (category, category_name, status)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM System_Container WHERE category_name = %s);
            ''', (category, category_name, status,category_name))

            # 如果记录已存在，则更新状态
            cur.execute('''
                UPDATE System_Container 
                SET status = %s
                WHERE category_name = %s;
            ''', (status, category_name))

            # 语言类容器池表
            category = f'Language'
            category_name = language_names[i]

            cur.execute('''
                INSERT INTO Language_Container (category, category_name, status)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM Language_Container WHERE category_name = %s);
            ''', (category, category_name, status, category_name))

            # 如果记录已存在，则更新状态
            cur.execute('''
                UPDATE Language_Container 
                SET status = %s
                WHERE category_name = %s;
            ''', (status, category_name))

            #工作类容器池表
            category = f'Work'
            category_name = work_names[i]

            cur.execute('''
                INSERT INTO Work_Container (category, category_name, status)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM Work_Container WHERE category_name = %s);
            ''', (category, category_name, status, category_name))

            # 如果记录已存在，则更新状态
            cur.execute('''
                UPDATE Work_Container 
                SET status = %s
                WHERE category_name = %s;
            ''', (status, category_name))

        for i in range(0, 6):
            child_ip_address = ip_addresses[i]
            child_name = child_names[i]
            category = categorys[i//2]

            #主节点表
            cur.execute('''
                    INSERT INTO Main_ip (child_ip_address)
                    SELECT %s
                    WHERE NOT EXISTS (SELECT 1 FROM Main_ip WHERE child_ip_address = %s);
                ''', (child_ip_address, child_ip_address))

            #主-子节点
            cur.execute('''
                    INSERT INTO Main_Child (main_child_ip_address,child_name)
                    SELECT %s,%s
                    WHERE NOT EXISTS (SELECT 1 FROM Main_Child WHERE main_child_ip_address = %s);
                ''', (child_ip_address,child_name, child_ip_address))

        # 子节点表
        temp1 = random.randint(4, 8)
        temp2 = 4096

        cur.execute('''
                    INSERT INTO ChildA_ip (current_ip_address,category,cpu_number,memory)
                    SELECT %s,%s,%s,%s
                    WHERE NOT EXISTS (SELECT 1 FROM ChildA_ip WHERE current_ip_address = %s);
                ''', (ip_addresses[0], categorys[0], temp1, temp2, ip_addresses[0]))
        temp1 = random.randint(4, 8)
        cur.execute('''
                    INSERT INTO ChildB_ip (current_ip_address,category,cpu_number,memory)
                    SELECT %s,%s,%s,%s
                    WHERE NOT EXISTS (SELECT 1 FROM ChildB_ip WHERE current_ip_address = %s);
                ''', (ip_addresses[1], categorys[0], temp1, temp2, ip_addresses[1]))
        temp1 = random.randint(4, 8)
        cur.execute('''
                    INSERT INTO ChildC_ip (current_ip_address,category,cpu_number,memory)
                    SELECT %s,%s,%s,%s
                    WHERE NOT EXISTS (SELECT 1 FROM ChildC_ip WHERE current_ip_address = %s);
                ''', (ip_addresses[2], categorys[1], temp1, temp2, ip_addresses[2]))
        temp1 = random.randint(4, 8)
        cur.execute('''
                    INSERT INTO ChildD_ip (current_ip_address,category,cpu_number,memory)
                    SELECT %s,%s,%s,%s
                    WHERE NOT EXISTS (SELECT 1 FROM ChildD_ip WHERE current_ip_address = %s);
                ''', (ip_addresses[3], categorys[1], temp1, temp2, ip_addresses[3]))
        temp1 = random.randint(4, 8)
        cur.execute('''
                    INSERT INTO ChildE_ip (current_ip_address,category,cpu_number,memory)
                    SELECT %s,%s,%s,%s
                    WHERE NOT EXISTS (SELECT 1 FROM ChildE_ip WHERE current_ip_address = %s);
                ''', (ip_addresses[4], categorys[2], temp1, temp2, ip_addresses[4]))
        temp1 = random.randint(4, 8)
        cur.execute('''
                    INSERT INTO ChildF_ip (current_ip_address,category,cpu_number,memory)
                    SELECT %s,%s,%s,%s
                    WHERE NOT EXISTS (SELECT 1 FROM ChildF_ip WHERE current_ip_address = %s);
                ''', (ip_addresses[5], categorys[2], temp1, temp2, ip_addresses[5]))


        for i in range(0, 20):
            #Task任务
            T_id = f'Task{i+1}'
            temp1 = random.randint(1, 3)
            temp2 = 1024
            status = random.choice(task_statuses)
            cur.execute('''
                    INSERT INTO Task (task_id,cpu_number,memory,status)
                    SELECT %s,%s,%s,%s
                    WHERE NOT EXISTS (SELECT 1 FROM Task WHERE task_id = %s);
                ''', (T_id, temp1, temp2, status, T_id))

        for i in range(0, 5):
            #Task_System表
            T_id = f'Task{i + 1}'
            category_name = system_names[i]
            cur.execute('''
                    INSERT INTO Task_System (task_id, category_name)
                    SELECT %s, %s
                    WHERE NOT EXISTS (SELECT 1 FROM Task_System WHERE task_id = %s);
                ''', (T_id, category_name, T_id))

            T_id = f'Task{i + 6}'
            category_name = language_names[i]
            cur.execute('''
                    INSERT INTO Task_Language (task_id, category_name)
                    SELECT %s, %s
                    WHERE NOT EXISTS (SELECT 1 FROM Task_Language WHERE task_id = %s);
                ''', (T_id, category_name, T_id))

            T_id = f'Task{i + 11}'
            category_name = work_names[i]
            cur.execute('''
                    INSERT INTO Task_Work (task_id, category_name)
                    SELECT %s, %s
                    WHERE NOT EXISTS (SELECT 1 FROM Task_Work WHERE task_id = %s);
                ''', (T_id, category_name, T_id))






        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")
    finally:
        if conn is not None:
            conn.close()



if __name__ == '__main__':
    create_tables()
    insert_data()
