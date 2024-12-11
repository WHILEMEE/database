from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route('/receive_data', methods=['POST'])
def receive_data():
    # 获取请求中的JSON数据
    data = request.json

    if not data:
        return jsonify({"error": "No JSON data provided"}), 400

    task = data.get('task')

    if not task:
        return jsonify({"error": "Missing 'task' key in JSON data"}), 400

    # 返回一个成功的响应
    response_data = {
        "message": f"Received task: {task}"
    }
    print(task)
    return jsonify(response_data), 200



if __name__ == '__main__':
    app.run(port=5001, debug=True)



