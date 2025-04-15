#!/usr/bin/env python3
import time
import socket
import signal
import sys
import threading
import socketserver
import subprocess
import os

# Configuration
HOST = "0.0.0.0"  # Listen on all interfaces
PORT = 8000       # Port to listen on

# Set up signal handling to gracefully shut down
running = True

def signal_handler(sig, frame):
    global running
    print("\nShutting down server...")
    running = False
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class CommandHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # This function is called whenever a client connects
        self.request.sendall(b"Container service is running!\n")

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True

def run_server():
    server = ThreadedTCPServer((HOST, PORT), CommandHandler)
    
    # Start the server thread
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    
    print(f"Server running on {HOST}:{PORT}")
    
    try:
        while running:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()

if __name__ == "__main__":
    # Print container info
    hostname = socket.gethostname()
    print(f"Container started with hostname: {hostname}")
    print(f"Container ID: {os.environ.get('HOSTNAME', 'unknown')}")
    
    # Keep the script running
    print("Container service started. Use docker exec to run commands.")
    print("Press Ctrl+C to stop.")
    
    run_server()