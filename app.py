from flask import Flask, request, render_template_string, jsonify
import requests
import datetime
app = Flask(__name__)

tasks=[]
@app.route('/receive_data', methods=['POST'])
def get_tasks():
    global tasks
    tasks=request.json
    response_data = {
        "message": f"Received"
    }
    print(tasks)
    return jsonify(response_data), 200

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        Lsystem = request.form.get('Lsystem')
        print(f"Received Lsystem: {Lsystem}")
        Llanguage = request.form.get('Llanguage')
        print(f"Received Llanguage: {Llanguage}")
        Lwork = request.form.get('Lwork')
        print(f"Received Lwork: {Lwork}")
        task=[Lsystem, Llanguage, Lwork]
        print(task)

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
    global tasks
    with open('template.html', 'r', encoding='utf-8') as file:
        template_content = file.read()
    return render_template_string(template_content, tasks=tasks)

@app.route('/update', methods=['GET'])
def update():
    global tasks
    return jsonify(tasks)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)