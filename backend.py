from flask import Flask, request, jsonify
import psycopg2
import requests
import time
import threading

app = Flask(__name__)

DB_CONFIG = {
    'dbname': 'School',
    'user': 'gaussdb',
    'password': 'Enmo@123',
    'host': '172.27.12.99',
    'port': '5432'
}


@app.route('/receive_data', methods=['POST'])
def receive_data():
    # 获取请求中的JSON数据
    data = request.json

    if not data:
        return jsonify({"error": "No JSON data provided"}), 400

    # 假设我们期望接收到一个包含'task'键的JSON对象
    task = data.get('task')

    if not task:
        return jsonify({"error": "Missing 'task' key in JSON data"}), 400

    # 返回一个成功的响应
    response_data = {
        "message": f"Received task: {task}"
    }
    print(task)
    return jsonify(response_data), 200

def post_data():
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute('SELECT task_id, cpu_number, memory, status FROM task;')
        tasks = [{'task_id': row[0], 'cpu_number': row[1], 'memory': row[2], 'status': row[3]} for row in cur.fetchall()]
        response=requests.post('http://127.0.0.1:5000/receive_data', json=tasks)
        if response.status_code == 200:
            print("Data posted successfully.")
        else:
            print(f"Failed to post data. Status code: {response.status_code}")
            print(response.text)
        print(tasks)
        time.sleep(5)

data_sender_thread = threading.Thread(target=post_data, daemon=True)
data_sender_thread.start()

if __name__ == '__main__':
    app.run(port=5001, debug=True)




