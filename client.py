import socket
import json

HOST = "127.0.0.1"
PORT = 9000

# Python 字典：描述注册请求
request = {
    "action": "registry",
    "peer_id": "peer_a",
    "host": "127.0.0.1",
    "port": 9001,
    "files": ["a.txt"],
}

# 字典 → JSON 文本，加换行符作为消息结束标记
message = json.dumps(request) + "\n"

with socket.create_connection((HOST, PORT)) as client:
    # 文本 → 字节，然后通过 TCP 发送
    client.sendall(message.encode("utf-8"))

    with client.makefile("r", encoding="utf-8") as reader:
        reply = reader.readline()
        print("Server replied:", reply.strip())
