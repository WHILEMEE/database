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
                System_container varchar(20),
                Language_container varchar(20),
                Work_container varchar(20),
                cpu_number int4,
                memory int4      
            );
        ''')

        # 子节点B
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ChildB_ip (
                current_ip_address varchar(20) NOT NULL,
                PRIMARY KEY (current_ip_address),
                System_container varchar(20),
                Language_container varchar(20),
                Work_container varchar(20),
                cpu_number int4,
                memory int4
            );
        ''')

        # 子节点C
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ChildC_ip (
                current_ip_address varchar(20) NOT NULL,
                PRIMARY KEY (current_ip_address),
                System_container varchar(20),
                Language_container varchar(20),
                Work_container varchar(20),
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

        
        # 创建ChildA_System_Container表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ChildA_System_Container (
                category varchar(20) NOT NULL,
                category_name varchar(20),
                PRIMARY KEY (category_name),
                status VARCHAR(50) CHECK (status IN ('active', 'unactive'))
            );       
        ''')

        # 创建ChildA_Language_Container表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ChildA_Language_Container (
                category varchar(20) NOT NULL,
                category_name varchar(20),
                PRIMARY KEY (category_name),
                status VARCHAR(50) CHECK (status IN ('active', 'unactive'))
            );           
        ''')

        # 创建ChildA_Work_Container表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ChildA_Work_Container (
                category varchar(20) NOT NULL,
                category_name varchar(20),
                PRIMARY KEY (category_name),
                status VARCHAR(50) CHECK (status IN ('active', 'unactive'))
            ); 
        ''')

        # 创建ChildB_System_Container表
        cur.execute('''
                    CREATE TABLE IF NOT EXISTS ChildB_System_Container (
                        category varchar(20) NOT NULL,
                        category_name varchar(20),
                        PRIMARY KEY (category_name),
                        status VARCHAR(50) CHECK (status IN ('active', 'unactive'))
                    );       
                ''')

        # 创建ChildB_Language_Container表
        cur.execute('''
                    CREATE TABLE IF NOT EXISTS ChildB_Language_Container (
                        category varchar(20) NOT NULL,
                        category_name varchar(20),
                        PRIMARY KEY (category_name),
                        status VARCHAR(50) CHECK (status IN ('active', 'unactive'))
                    );           
                ''')

        # 创建ChildB_Work_Container表
        cur.execute('''
                    CREATE TABLE IF NOT EXISTS ChildB_Work_Container (
                        category varchar(20) NOT NULL,
                        category_name varchar(20),
                        PRIMARY KEY (category_name),
                        status VARCHAR(50) CHECK (status IN ('active', 'unactive'))
                    ); 
                ''')

        # 创建ChildC_System_Container表
        cur.execute('''
                    CREATE TABLE IF NOT EXISTS ChildC_System_Container (
                        category varchar(20) NOT NULL,
                        category_name varchar(20),
                        PRIMARY KEY (category_name),
                        status VARCHAR(50) CHECK (status IN ('active', 'unactive'))
                    );       
                ''')

        # 创建ChildC_Language_Container表
        cur.execute('''
                    CREATE TABLE IF NOT EXISTS ChildC_Language_Container (
                        category varchar(20) NOT NULL,
                        category_name varchar(20),
                        PRIMARY KEY (category_name),
                        status VARCHAR(50) CHECK (status IN ('active', 'unactive'))
                    );           
                ''')

        # 创建ChildC_Work_Container表
        cur.execute('''
                    CREATE TABLE IF NOT EXISTS ChildC_Work_Container (
                        category varchar(20) NOT NULL,
                        category_name varchar(20),
                        PRIMARY KEY (category_name),
                        status VARCHAR(50) CHECK (status IN ('active', 'unactive'))
                    ); 
                ''')



        #创建ChildA_Task表
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ChildA_Task (
                Task_id varchar(20) NOT NULL,
                PRIMARY KEY (Task_id),
                cpu_number int4,
                memory int4,
                status VARCHAR(50) CHECK (status IN ('failed','queued','completed','running')),
                System_container varchar(20),
                Language_container varchar(20),
                Work_container varchar(20)
            );
            
        ''')

        # 创建ChildB_Task表
        cur.execute('''
                    CREATE TABLE IF NOT EXISTS ChildB_Task (
                        Task_id varchar(20) NOT NULL,
                        PRIMARY KEY (Task_id),
                        cpu_number int4,
                        memory int4,
                        status VARCHAR(50) CHECK (status IN ('failed','queued','completed','running')),
                        System_container varchar(20),
                        Language_container varchar(20),
                        Work_container varchar(20)
                    );

                ''')

        # 创建ChildC_Task表
        cur.execute('''
                    CREATE TABLE IF NOT EXISTS ChildC_Task (
                        Task_id varchar(20) NOT NULL,
                        PRIMARY KEY (Task_id),
                        cpu_number int4,
                        memory int4,
                        status VARCHAR(50) CHECK (status IN ('failed','queued','completed','running')),
                        System_container varchar(20),
                        Language_container varchar(20),
                        Work_container varchar(20)
                    );

                ''')

        # 创建Total_Task表
        cur.execute('''
                CREATE TABLE IF NOT EXISTS Total_Task (
                    Task_id varchar(20) NOT NULL,
                    PRIMARY KEY (Task_id),
                    Start_Time TIMESTAMP NULL, -- 开始时间，初始为NULL，任务开始时更新
                    End_Time TIMESTAMP NULL   -- 结束时间，初始为NULL，任务完成时更新
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
    child_names = ['ChildA_ip', 'ChildB_ip', 'ChildC_ip']
    ip_addresses = ['111.111.111.1','111.111.111.2','111.111.111.3']
    system_names = ['Windows','MacOS','Unix','Linux','DOS']
    language_names = ['Java','Python','C++','C#','JavaScript']
    work_names = ['MySQL','PostgreSQL','Oracle','Sybase','Clipper']
    container_statuses = ['active', 'unactive']
    task_statuses = ['queued','running','completed','failed']
    categorys = ['System','Language','Work']

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
#每个容器池表放入20个数据

        for i in range(0, 5):
            category = f'System'
            category_name = system_names[i]
            status = container_statuses[0]

            # 尝试插入新记录
            # 先是系统类容器池表
            cur.execute('''
                INSERT INTO ChildA_System_Container (category, category_name, status)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM ChildA_System_Container WHERE category_name = %s);
            ''', (category, category_name, status,category_name))

            cur.execute('''
                INSERT INTO ChildB_System_Container (category, category_name, status)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM ChildB_System_Container WHERE category_name = %s);
            ''', (category, category_name, status, category_name))

            cur.execute('''
                INSERT INTO ChildC_System_Container (category, category_name, status)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM ChildC_System_Container WHERE category_name = %s);
            ''', (category, category_name, status, category_name))


            # 语言类容器池表
            category = f'Language'
            category_name = language_names[i]

            cur.execute('''
                INSERT INTO ChildA_Language_Container (category, category_name, status)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM ChildA_Language_Container WHERE category_name = %s);
            ''', (category, category_name, status, category_name))

            cur.execute('''
                    INSERT INTO ChildB_Language_Container (category, category_name, status)
                    SELECT %s, %s, %s
                    WHERE NOT EXISTS (SELECT 1 FROM ChildB_Language_Container WHERE category_name = %s);
                ''', (category, category_name, status, category_name))

            cur.execute('''
                    INSERT INTO ChildC_Language_Container (category, category_name, status)
                    SELECT %s, %s, %s
                    WHERE NOT EXISTS (SELECT 1 FROM ChildC_Language_Container WHERE category_name = %s);
                ''', (category, category_name, status, category_name))


            #工作类容器池表
            category = f'Work'
            category_name = work_names[i]

            cur.execute('''
                INSERT INTO ChildA_Work_Container (category, category_name, status)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM ChildA_Work_Container WHERE category_name = %s);
            ''', (category, category_name, status, category_name))

            cur.execute('''
                INSERT INTO ChildB_Work_Container (category, category_name, status)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM ChildB_Work_Container WHERE category_name = %s);
            ''', (category, category_name, status, category_name))

            cur.execute('''
                INSERT INTO ChildC_Work_Container (category, category_name, status)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (SELECT 1 FROM ChildC_Work_Container WHERE category_name = %s);
            ''', (category, category_name, status, category_name))


        for i in range(0, 3):
            child_ip_address = ip_addresses[i]
            child_name = child_names[i]

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
                    INSERT INTO ChildA_ip (current_ip_address,System_container,Language_container,Work_container,cpu_number,memory)
                    SELECT %s,%s,%s,%s,%s,%s
                    WHERE NOT EXISTS (SELECT 1 FROM ChildA_ip WHERE current_ip_address = %s);
                ''', (ip_addresses[0], categorys[0],categorys[1],categorys[2], temp1, temp2, ip_addresses[0]))
        temp1 = random.randint(4, 8)
        cur.execute('''
                    INSERT INTO ChildB_ip (current_ip_address,System_container,Language_container,Work_container,cpu_number,memory)
                    SELECT %s,%s,%s,%s,%s,%s
                    WHERE NOT EXISTS (SELECT 1 FROM ChildB_ip WHERE current_ip_address = %s);
                ''', (ip_addresses[1], categorys[0],categorys[1],categorys[2], temp1, temp2, ip_addresses[1]))
        temp1 = random.randint(4, 8)
        cur.execute('''
                    INSERT INTO ChildC_ip (current_ip_address,System_container,Language_container,Work_container,cpu_number,memory)
                    SELECT %s,%s,%s,%s,%s,%s
                    WHERE NOT EXISTS (SELECT 1 FROM ChildC_ip WHERE current_ip_address = %s);
                ''', (ip_addresses[2], categorys[0],categorys[1],categorys[2], temp1, temp2, ip_addresses[2]))

        for i in range(0, 9):
            #Task任务
            T_id = f'Task{i+1}'
            cur.execute('''
                    INSERT INTO Total_Task (task_id)
                    SELECT %s
                    WHERE NOT EXISTS (SELECT 1 FROM Total_Task WHERE task_id = %s);
                ''', (T_id, T_id))

        for i in range(0,3):
            #ChildA_Task
            T_id = f'Task{i+1}'
            temp1 = random.randint(1, 3)
            temp2 = 1024
            status = task_statuses[0]
            cur.execute('''
                INSERT INTO ChildA_Task (task_id,cpu_number,memory,status,System_container,Language_container,Work_container)
                SELECT %s,%s,%s,%s,%s,%s,%s
                WHERE NOT EXISTS (SELECT 1 FROM ChildA_Task WHERE task_id = %s);
            ''', (T_id, temp1, temp2, status, random.choice(system_names), random.choice(language_names),random.choice(work_names), T_id))

        for i in range(0,3):
            #ChildA_Task
            T_id = f'Task{i+1}'
            temp1 = random.randint(1, 3)
            temp2 = 1024
            status = task_statuses[0]
            cur.execute('''
                INSERT INTO ChildB_Task (task_id,cpu_number,memory,status,System_container,Language_container,Work_container)
                SELECT %s,%s,%s,%s,%s,%s,%s
                WHERE NOT EXISTS (SELECT 1 FROM ChildB_Task WHERE task_id = %s);
            ''', (T_id, temp1, temp2, status, random.choice(system_names), random.choice(language_names),random.choice(work_names), T_id))

        for i in range(0,3):
            #ChildA_Task
            T_id = f'Task{i+1}'
            temp1 = random.randint(1, 3)
            temp2 = 1024
            status = task_statuses[0]
            cur.execute('''
                INSERT INTO ChildC_Task (task_id,cpu_number,memory,status,System_container,Language_container,Work_container)
                SELECT %s,%s,%s,%s,%s,%s,%s
                WHERE NOT EXISTS (SELECT 1 FROM ChildC_Task WHERE task_id = %s);
            ''', (T_id, temp1, temp2, status, random.choice(system_names), random.choice(language_names),random.choice(work_names), T_id))


        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")
    finally:
        if conn is not None:
            conn.close()

def trigger():
    """创建所需的数据库表"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 创建或替换触发器函数，以在更新时自动设置 End_Time
        cur.execute('''
                    CREATE OR REPLACE FUNCTION update_task_end_time()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        IF NEW.Status = 'completed' THEN
                            UPDATE Total_Task
                            SET End_Time = CURRENT_TIMESTAMP
                            WHERE Task_id = NEW.Task_id;
                        END IF;
                        RETURN NEW; -- 确保返回 NEW 记录
                    END;
                    $$ LANGUAGE plpgsql;
                ''')

        # 检查并创建触发器，如果不存在的话
        # 辅助函数：检查触发器是否存在
        def trigger_exists(cur, trigger_name, table_name):
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM pg_trigger 
                    JOIN pg_class ON pg_trigger.tgrelid = pg_class.oid 
                    WHERE pg_class.relname = %s AND pg_trigger.tgname = %s
                );
            """, (table_name, trigger_name))
            return cur.fetchone()[0]

        # 创建触发器，如果不存在的话
        tables_and_triggers = [
            ('ChildA_Task', 'childA_task_status_change'),
            ('ChildB_Task', 'childB_task_status_change'),
            ('ChildC_Task', 'childC_task_status_change')
        ]

        for table_name, trigger_name in tables_and_triggers:
            if not trigger_exists(cur, trigger_name, table_name):
                cur.execute(f'''
                    CREATE TRIGGER {trigger_name}
                    AFTER UPDATE OF Status ON {table_name}
                    FOR EACH ROW
                    WHEN (OLD.Status IS DISTINCT FROM NEW.Status)
                    EXECUTE PROCEDURE update_task_end_time();
                ''')

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
     create_tables()
     insert_data()
     trigger()
