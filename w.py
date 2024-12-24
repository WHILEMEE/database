import threading
from flask import Flask, request, jsonify
import psycopg2
import random
import time
from queue import Queue
from datetime import datetime
from threading import Thread
DB_CONFIG = {
    'dbname': 'School',
    'user': 'gaussdb',
    'password': 'Enmo@123',
    'host': '172.27.12.99',
    'port': '5432'
}
app = Flask(__name__)
# 假设任务队列已经定义并通过 POST 请求添加
task_queue = Queue()
@app.route('/receive_data', methods=['POST'])
def receive_data():
    # 获取请求中的JSON数据
    data = request.json
    task_queue.put(data)

    if not data:
        return jsonify({"error": "No JSON data provided"}), 400
    # 返回一个成功的响应
    response_data = {
        "message": f"Received task"
    }
    print(data)
    return jsonify(response_data), 200

def connect_db():
    """连接到数据库"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database connection error: {error}")
        return None


def childA_match_score(language_req,system_req,work_req):
    """计算节点A的匹配度"""
    conn = connect_db()
    if conn:
        try:
            cur = conn.cursor()
            # 获取任务的容器需求
            cur.execute(f"""
                SELECT * FROM "childa_language_container"
                WHERE status = 'active';
            """)
            language_nodes = cur.fetchall()
            language_score = 0
            i=0
            while i < 5:
                if language_nodes[i] == language_req:
                    language_score = language_score + 1
                    break
                else:
                    i=i+1
            cur.execute(f"""
                            SELECT * FROM "childa_system_container"
                            WHERE status = 'active';
                        """)
            system_nodes = cur.fetchall()
            system_score = 0
            i = 0
            while i < 5:
                if system_nodes[i] == system_req:
                    system_score = system_score + 1
                    break
                else:
                    i = i + 1
            cur.execute(f"""
                            SELECT * FROM "childa_work_container"
                            WHERE status = 'active';
                        """)
            work_nodes = cur.fetchall()
            work_score = 0
            i = 0
            while i < 5:
                if work_nodes[i] == work_req:
                    work_score = work_score + 1
                    break
                else:
                    i = i + 1
            total_score = language_score + system_score + work_score
            return total_score
        finally:
            conn.close()
    return None
def childB_match_score(language_req,system_req,work_req):
    """计算节点B的匹配度"""
    conn = connect_db()
    if conn:
        try:
            cur = conn.cursor()
            # 获取任务的容器需求
            cur.execute(f"""
                SELECT * FROM "childb_language_container"
                WHERE status = 'active';
            """)
            language_nodes = cur.fetchall()
            language_score = 0
            i=0
            while i < 5:
                if language_nodes[i] == language_req:
                    language_score = language_score + 1
                    break
                else:
                    i=i+1
            cur.execute(f"""
                            SELECT * FROM "childb_system_container"
                            WHERE status = 'active';
                        """)
            system_nodes = cur.fetchall()
            system_score = 0
            i = 0
            while i < 5:
                if system_nodes[i] == system_req:
                    system_score = system_score + 1
                    break
                else:
                    i = i + 1
            cur.execute(f"""
                            SELECT * FROM "childb_work_container"
                            WHERE status = 'active';
                        """)
            work_nodes = cur.fetchall()
            work_score = 0
            i = 0
            while i < 5:
                if work_nodes[i] == work_req:
                    work_score = work_score + 1
                    break
                else:
                    i = i + 1
            total_score = language_score + system_score + work_score
            return total_score
        finally:
            conn.close()
    return None
def childC_match_score(language_req,system_req,work_req):
    """计算节点C的匹配度"""
    conn = connect_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT * FROM "childc_language_container"
                WHERE status = 'active';
            """)
            language_nodes = cur.fetchall()
            language_score = 0
            i=0
            while i < 5:
                if language_nodes[i] == language_req:
                    language_score = language_score + 1
                    break
                else:
                    i=i+1
            cur.execute(f"""
                            SELECT * FROM "childc_system_container"
                            WHERE status = 'active';
                        """)
            system_nodes = cur.fetchall()
            system_score = 0
            i = 0
            while i < 5:
                if system_nodes[i] == system_req:
                    system_score = system_score + 1
                    break
                else:
                    i = i + 1
            cur.execute(f"""
                            SELECT * FROM "childc_work_container"
                            WHERE status = 'active';
                        """)
            work_nodes = cur.fetchall()
            work_score = 0
            i = 0
            while i < 5:
                if work_nodes[i] == work_req:
                    work_score = work_score + 1
                    break
                else:
                    i = i + 1
            total_score = language_score + system_score + work_score
            return total_score
        finally:
            conn.close()
    return None
def update_node_tasks(task_id,child_name,task_cpu,task_memory,language_req,system_req,work_req ):
    """将任务插入到对应的子节点任务表"""
    conn = connect_db()
    if conn:
        try:
            cur = conn.cursor()
            # 更新容器
            cur.execute(f"""
                insert into {child_name}_task (task_id,cpu_number,memory,status,system_container,language_container,work_container)
                values (%s,'%s','%s','running',%s,%s,%s);""",(task_id,task_cpu,task_memory,system_req,language_req,work_req))
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error updating containers for {child_name}_task: {error}")
        finally:
            conn.close()
def update_task_end_time(task_id):
    """更新任务的结束时间"""
    conn = connect_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE Total_Task
                SET End_Time = %s
                WHERE Task_id = %s
                RETURNING *;
            """, (datetime.now(), task_id))
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error updating task end time: {error}")
        finally:
            conn.close()
def update_task_start_time(task_id):
    """更新任务的开始时间"""
    conn = connect_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE Total_Task
                SET Start_Time = %s
                WHERE Task_id = %s
                RETURNING *;
            """, (datetime.now(), task_id))
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error updating task start time: {error}")
        finally:
            conn.close()
def allocating_resource(table_name,available_cpu,available_memory,task_cpu,task_memory):
    """分配节点资源"""
    conn = connect_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(f"""
                UPDATE {table_name}_ip
                SET cpu_number = %s,
                    memory = %s
                RETURNING *;""",((available_cpu-task_cpu),(available_memory-task_memory)))
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error updating resource error: {error}")
        finally:
            conn.close()
def get_available_cpu(table_name):
    conn = connect_db()
    if conn:
        try:
            cur = conn.cursor()
            if not table_name.endswith('_ip'):
                ntable_name = table_name + '_ip'
            else:
                ntable_name = table_name
            cur.execute(f"""
                select cpu_number
                from {ntable_name};""")
            cpu = cur.fetchall()
            cpudd=cpu[0][0]
            return cpudd
        finally:
            conn.close()
    else:
        return None
def get_available_memory(table_name):
    conn = connect_db()
    if conn:
        try:
            cur = conn.cursor()
            if not table_name.endswith('_ip'):
                ntable_name = table_name + '_ip'
            else:
                ntable_name = table_name
            cur.execute(f"""
                select memory from {ntable_name};""")
            memory = cur.fetchall()
            memorydd=memory[0][0]
            return memorydd
        finally:
            conn.close()
    else:
        return None
def Release_resources(table_name,available_cpu,available_memory,task_cpu,task_memory):
    time.sleep(5)
    conn = connect_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(f"""
                UPDATE {table_name}_ip
                SET cpu_number = %s,
                    memory = %s
                RETURNING *;""",((available_cpu+task_cpu),(available_memory+task_memory)))
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error updating resource error: {error}")
        finally:
            conn.close()
    update_task_status(table_name)
def update_task_status(child_name):
    conn =connect_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(f"""
                update {child_name}_task set status = 'running'""")
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error updating task end time: {error}")
        finally:
            conn.close()
def schedule_tasks():
    while True:
        if not task_queue.empty():
            task = task_queue.get()
            task_id = task['task_id']
            cpu_req = int(task['cpu'])
            memory_req = int(task['memory'])
            language_req = task['language_container']
            system_req = task['system_container']
            work_req = task['work_container']
            Childa_ip = 'childa_ip'
            Childb_ip = 'childb_ip'
            Childc_ip = 'childc_ip'
            work_child = 'x'
            # 记录任务开始时间
            update_task_start_time(task_id)

            # 计算节点A的匹配度
            matchA = childA_match_score(language_req,system_req,work_req)

            #计算节点B的匹配度
            matchB = childB_match_score(language_req,system_req,work_req)

            #计算节点C的匹配度
            matchC = childC_match_score(language_req,system_req,work_req)
            if matchA == max(matchA,matchB,matchC):
                if (cpu_req <= get_available_cpu(Childa_ip)) and (memory_req <= get_available_memory(Childa_ip)):
                    work_child = 'Childa'
                else:
                    if matchB >=matchC and cpu_req <= get_available_cpu(Childb_ip) and memory_req <= get_available_memory(Childb_ip):
                        work_child = 'Childb'
                    else:
                        if cpu_req <= get_available_cpu(Childc_ip) and memory_req <= get_available_memory(Childc_ip):
                            work_child = 'Childc'
                        else:
                            print('任务失败')
            if matchB == max(matchA,matchB,matchC):
                if (cpu_req <= get_available_cpu(Childb_ip)) and (memory_req <= get_available_memory(Childb_ip)):
                    work_child = 'Childb'
                else:
                    if matchA >= matchC and cpu_req <= get_available_cpu(Childa_ip) and memory_req <= get_available_memory(Childa_ip):
                        work_child = 'Childa'
                    else:
                        if cpu_req <= get_available_cpu(Childc_ip) and memory_req <= get_available_memory(Childc_ip):
                            work_child = 'Childc'
                        else:
                            print('任务失败')
            if matchC == max(matchA,matchB,matchC):
                if (cpu_req <= get_available_cpu(Childc_ip)) and (memory_req <= get_available_memory(Childc_ip)):
                    work_child = 'Childc'
                else:
                    if matchA >= matchB and cpu_req <= get_available_cpu(Childa_ip) and memory_req <= get_available_memory(Childa_ip):
                        work_child = 'Childa'
                    else:
                        if cpu_req <= get_available_cpu(Childb_ip) and memory_req <= get_available_memory(Childb_ip):
                            work_child = 'Childb'
                        else:
                            print('任务失败')
            update_node_tasks(task_id,work_child,cpu_req,memory_req,system_req,language_req, work_req)
            print('任务在节点{}上执行'.format(work_child))
            allocating_resource(work_child,get_available_cpu(work_child),get_available_memory(work_child),cpu_req,memory_req)
            release_thread = threading.Thread(target=Release_resources(work_child,get_available_cpu(work_child),get_available_memory(work_child),cpu_req,memory_req))
            release_thread.start()
            update_task_end_time(task_id)
data_thread = Thread(target=schedule_tasks, daemon=True)
data_thread.start()
if __name__ == '__main__':
    app.run(port=5001, debug=True)
