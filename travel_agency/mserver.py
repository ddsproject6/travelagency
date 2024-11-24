import socket
import threading
import json
import time


class MasterServer:
    def __init__(self, config_file='mserver_config.json'):
        self.master_address = None
        self.master_port = None
        self.chunk_servers = []  # List of chunk servers
        self.server_loads = {}  # Dictionary to store server loads
        self.server_status = {}  # Dictionary to track server health
        self.file_chunk_mapping = {}  # Mapping files to chunk servers
        self.start_health_check()

        self.load_config(config_file)
        self.start_server()


    def load_config(self, config_file):
        """Load configuration from a JSON file."""
        try:
            with open(config_file, 'r') as file:
                config = json.load(file)
            self.master_address = config['master_server']['address']
            self.master_port = config['master_server']['port']
            self.chunk_servers = config['chunk_servers']

            # Initialize server loads and status
            for server in self.chunk_servers:
                server_key = server['name']
                self.server_loads[server_key] = 0  # Initial load is zero
                self.server_status[server_key] = True  # Assume servers are healthy at start

            print(f"Configuration loaded from {config_file}")

            # Load existing file-to-chunk-server mappings
            self.load_file_chunk_mapping()
        except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
            print(f"Error loading configuration: {e}")
            raise

    def load_file_chunk_mapping(self):
        """Load file-to-primary mappings from a metadata file."""
        try:
            with open("files_metadata.json", "r") as f:
                files_metadata = json.load(f)

            for file_name, file_info in files_metadata.items():
                primary = file_info.get("primary")
                if primary:
                    primary_server = next(
                        (server for server in self.chunk_servers if server["name"] == primary),
                        None
                    )
                    if primary_server:
                        self.file_chunk_mapping[file_name] = f"{primary_server['address']}:{primary_server['port']}"
            print("File-to-primary mappings initialized:", self.file_chunk_mapping)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading file metadata: {e}")

    def start_server(self):
        """Start the master server."""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.bind((self.master_address, self.master_port))
            self.server_socket.listen(5)
            print(f"Master Server started at {self.master_address}:{self.master_port}")

            while True:
                conn, addr = self.server_socket.accept()
                print(f"Connected by {addr}")
                client_thread = threading.Thread(target=self.handle_client, args=(conn,))
                client_thread.start()
        except Exception as e:
            print(f"Error starting Master Server: {e}")

    def handle_client(self, conn):
        """Handle client requests."""
        try:
            data = conn.recv(4096)
            if not data:
                print("No data received from client.")
                return

            request = json.loads(data.decode('utf-8'))
            request_type = request.get("type")
            file_name = request.get("file_name")

            if request_type == "read":
                print(f"Received read request for file: {file_name}")
                chunk_server_address = self.get_chunk_server_for_file(file_name, is_write=False)
                if chunk_server_address:
                    conn.sendall(chunk_server_address.encode())
                    print(f"Sent chunk server address to client: {chunk_server_address}")
                else:
                    conn.sendall(b"Error: File not found")

            elif request_type == "write":
                print(f"Received write request for file: {file_name}")
                chunk_server_address = self.get_chunk_server_for_file(file_name, is_write=True)
                if chunk_server_address:
                    response = json.dumps({
                        "address": chunk_server_address.split(":")[0],
                        "port": int(chunk_server_address.split(":")[1])
                    })
                    conn.sendall(response.encode())
                    print(f"Sent primary server address to client: {chunk_server_address}")
                else:
                    conn.sendall(b"Error: No available chunk server for writing.")

        except Exception as e:
            print(f"Error handling client request: {e}")
        finally:
            conn.close()

    def get_chunk_server_for_file(self, file_name, is_write=False):
        """Determine the appropriate chunk server for a file."""
        if is_write:
            primary_address = self.file_chunk_mapping.get(file_name)
            if not primary_address:
                primary_server = self.select_primary_server(file_name)
                if primary_server:
                    primary_address = f"{primary_server['address']}:{primary_server['port']}"
                    self.file_chunk_mapping[file_name] = primary_address
                    self.notify_primary_server(primary_server, file_name)
            return primary_address
        else:
            return self.select_any_server(file_name)

    def select_primary_server(self, file_name):
        """Select a primary server for a file."""
        available_servers = [server for server in self.chunk_servers if self.server_status[server['name']]]
        if available_servers:
            least_loaded_server = min(available_servers, key=lambda server: self.server_loads[server['name']])
            self.server_loads[least_loaded_server['name']] += 1
            return least_loaded_server
        print("No available servers to assign as primary.")
        return None

    def select_any_server(self, file_name):
        """Select any available server for reading a file."""
        try:
            with open("files_metadata.json", "r") as f:
                files_metadata = json.load(f)

            replicas = files_metadata.get(file_name, {}).get("replicas", [])
            available_servers = [
                server for server in self.chunk_servers
                if server['name'] in replicas and self.server_status[server['name']]
            ]

            if available_servers:
                least_loaded_server = min(available_servers, key=lambda server: self.server_loads[server['name']])
                self.server_loads[least_loaded_server['name']] += 1
                return f"{least_loaded_server['address']}:{least_loaded_server['port']}"
        except (FileNotFoundError, json.JSONDecodeError):
            print(f"Metadata for file {file_name} not found.")
        print("No available replicas for the file.")
        return None

    def notify_primary_server(self, primary_server, file_name):
        """Notify a primary server about its assignment."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((primary_server['address'], primary_server['port']))
                notification = json.dumps({"type": "primary_assignment", "file_name": file_name})
                s.sendall(notification.encode())
                print(f"Primary server {primary_server['name']} notified for file: {file_name}")
        except Exception as e:
            print(f"Failed to notify primary server {primary_server['name']}: {e}")

    def start_health_check(self):
        """Start a thread for server health checks."""
        health_check_thread = threading.Thread(target=self.check_server_health, daemon=True)
        health_check_thread.start()

    def check_server_health(self):
        """Periodically check the health of chunk servers and print their statuses."""
        print("Health check thread started.")
        while True:
            print("\n--- Health Check Status ---")
            for server in self.chunk_servers:
                server_name = server['name']
                server_address = (server['address'], server['port'])
                try:
                    # Step 1: Establish connection to the server
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(2)
                    s.connect(server_address)

                    # Step 2: Send a ping request
                    ping_request = json.dumps({"type": "ping"}).encode()
                    s.sendall(ping_request)

                    # Step 3: Receive the response
                    response = s.recv(1024).decode()
                    if response == "pong":
                        if not self.server_status[server_name]:
                            print(f"Server {server_name} is back online.")
                        self.server_status[server_name] = True
                    else:
                        raise Exception("Invalid response")

                    s.close()

                except Exception as e:
                    if self.server_status[server_name]:
                        print(f"Server {server_name} is down: {e}")
                    self.server_status[server_name] = False

            # Print the health status of all servers
            for server_name, status in self.server_status.items():
                status_str = "Healthy" if status else "Down"
                print(f"Server {server_name}: {status_str}")

            print("---------------------------\n")
            time.sleep(10)  # Check every 10 seconds



if __name__ == "__main__":
    master_server = MasterServer(config_file='mserver_config.json')
