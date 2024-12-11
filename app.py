from flask import Flask, request, render_template_string, jsonify
import requests
import psycopg2
import json
import datetime
app = Flask(__name__)

DB_CONFIG = {
    'dbname': 'School',
    'user': 'gaussdb',
    'password': 'Enmo@123',
    'host': '172.27.12.99',
    'port': '5432'
}

def get_tasks():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute('SELECT task_id, cpu_number, memory, status FROM task;')
    tasks = [{'task_id': row[0], 'cpu_number': row[1], 'memory': row[2], 'status': row[3]} for row in cur.fetchall()]
    conn.close()
    return tasks

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        task = request.form.get('task')
        print(f"Received task: {task}")
        cpu = request.form.get('cpu')
        print(f"Received CPU: {cpu}")
        memory = request.form.get('memory')
        print(f"Received memory: {memory}")
        data = {
            "time": datetime.datetime.now().isoformat(),
            "task": task,
            "cpu": cpu,
            "memory": memory
        }
        requests.post('http://127.0.0.1:5001/receive_data', json=data)
        #response = requests.post('http://127.0.0.1:5001/receive_data', json=data)
        #if response.status_code == 200:
            #response_data = response.json()
            #return jsonify(response_data), 200
        #else:
            #return jsonify({"error": "Failed to send data to app1"}), 500

        #with open('data.json', 'a') as json_file:
            #json.dump(data, json_file)
        # 这里可以添加任务到数据库或者执行其他操作

    tasks = get_tasks()
    with open('template.html', 'r', encoding='utf-8') as file:
        template_content = file.read()
    return render_template_string(template_content, tasks=tasks)

@app.route('/update', methods=['GET'])
def update():
    tasks = get_tasks()
    return jsonify(tasks)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)