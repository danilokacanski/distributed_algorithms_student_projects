import asyncio
import json
import socket
import websockets
import http.server
import socketserver
import threading
import sys

# Store the latest state of each node
nodes_state = {}
clients = set()

def start_http_server():
    Handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", 8080), Handler) as httpd:
        print("Serving UI on http://localhost:8080")
        httpd.serve_forever()

async def udp_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", 9000))
    sock.setblocking(False)
    
    loop = asyncio.get_running_loop()
    print("Listening for UDP Telemetry on port 9000...")
    
    while True:
        data, addr = await loop.sock_recvfrom(sock, 1024)
        try:
            payload = json.loads(data.decode('utf-8'))
            node_id = payload.get("id")
            nodes_state[node_id] = payload
            
            # Broadcast to all connected websocket clients
            if clients:
                message = json.dumps(payload)
                await asyncio.gather(*[client.send(message) for client in clients])
        except Exception as e:
            pass

async def websocket_handler(websocket):
    clients.add(websocket)
    try:
        # Send current state immediately upon connection
        await websocket.send(json.dumps({"type": "init", "state": nodes_state}))
        await websocket.wait_closed()
    finally:
        clients.remove(websocket)

async def main():
    # Start HTTP server in a background thread to serve index.html
    threading.Thread(target=start_http_server, daemon=True).start()
    
    # Start WebSocket server
    start_ws = websockets.serve(websocket_handler, "0.0.0.0", 8081)
    
    # Run UDP receiver and WS server concurrently
    await asyncio.gather(
        udp_server(),
        start_ws
    )

if __name__ == "__main__":
    asyncio.run(main())
