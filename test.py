import psycopg2
import random
import time
from queue import Queue
from datetime import datetime
import requests
from flask import Flask, request, jsonify

from threading import Thread

app = Flask(__name__)

DB_CONFIG = {
    'dbname': 'School',
    'user': 'gaussdb',
    'password': 'Enmo@123',
    'host': '172.27.12.99',
    'port': '5432'
}

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


def get_best_matching_node(task):
    """根据任务的容器需求，查找匹配度最高的子节点"""
    conn = connect_db()
    if conn:
        try:
            cur = conn.cursor()
            # 获取任务的容器需求
            system_req = task['System_container']
            language_req = task['Language_container']
            work_req = task['Work_container']

            # 查询三个子节点表，评估匹配度
            matching_scores = {
                'ChildA': 0,
                'ChildB': 0,
                'ChildC': 0
            }

            # 计算每个节点的匹配度
            for child in ['ChildA', 'ChildB', 'ChildC']:
                cur.execute(f"""
                    SELECT * FROM {child}_ip 
                    WHERE current_ip_address IS NOT NULL;
                """)
                nodes = cur.fetchall()

                for node in nodes:
                    system_container = node[2]
                    language_container = node[3]
                    work_container = node[4]

                    score = 0
                    if system_container == system_req:
                        score += 1
                    if language_container == language_req:
                        score += 1
                    if work_container == work_req:
                        score += 1

                    matching_scores[child] += score

            # 选择匹配度最高的节点
            best_node = max(matching_scores, key=matching_scores.get)
            return best_node
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error getting best matching node: {error}")
        finally:
            conn.close()
    return None


def update_node_containers(child_name, task):
    """更新子节点的容器为任务所需的容器"""
    conn = connect_db()
    if conn:
        try:
            cur = conn.cursor()
            # 更新容器
            cur.execute(f"""
                UPDATE {child_name}_ip
                SET system_container = %s,
                    language_container = %s,
                    work_container = %s
                    WHERE current_ip_address IN (SELECT main_child_ip_address FROM Main_Child WHERE child_name = %s);
            """, (task['System_container'], task['Language_container'], task['Work_container'], child_name))
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error updating containers for {child_name}: {error}")
        finally:
            conn.close()


def schedule_tasks():
    """调度任务并执行"""
    while True:
        if not task_queue.empty():
            task = task_queue.get()
            task_id = task['Task_id']
            # 记录任务开始时间
            update_task_start_time(task_id)

            # 查找最匹配的节点
            best_node = get_best_matching_node(task)
            if best_node:
                # 更新容器
                update_node_containers(best_node, task)
                print(f"Task {task_id} scheduled to {best_node}")

                # 记录任务完成时间
                update_task_end_time(task_id)
            else:
                print(f"No matching node found for Task {task_id}")

        time.sleep(2)  # 每隔2秒检查一次任务队列
data_thread = Thread(target=schedule_tasks, daemon=True)
data_thread.start()

if __name__ == '__main__':
    # 启动任务调度器
    app.run(port=5001, debug=True)


