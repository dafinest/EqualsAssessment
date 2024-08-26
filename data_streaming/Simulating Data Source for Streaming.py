import socket
import time
import json
import random

# Set up socket connection
HOST = 'localhost'  # IP address of the server (localhost in this case)
PORT = 9999  # Port number

# Predefined transaction types
transaction_types = ['withdrawal', 'deposit', 'purchase']

# Simulate sending data
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(1)
    print(f"Listening on {HOST}:{PORT}...")
    
    conn, addr = s.accept()
    with conn:
        print(f"Connected by {addr}")
        while True:
            # Generate random transaction data
            transaction = {
                "transaction_id": random.randint(1, 1000),
                "transaction_amount": round(random.uniform(10, 1000), 2),
                "transaction_type": random.choice(transaction_types)
            }
            
            # Convert transaction to JSON string
            transaction_json = json.dumps(transaction)
            
            # Send the data to the client (Spark Streaming in this case)
            conn.sendall(bytes(transaction_json + "\n", encoding='utf-8'))
            
            # Simulate delay between transactions
            time.sleep(1)
