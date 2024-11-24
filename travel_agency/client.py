import socket
import json
import time

class Client:
    def __init__(self, config_file='client_config.json'):
        self.master_address = None
        self.master_port = None
        self.client_socket = None
        self.retry_attempts = 5  # Set max retry attempts or -1 for infinite retries
        self.retry_delay = 3  # Seconds to wait before retrying
        self.temp_file_path = 'temp_output_file.txt'  # Path to store the temporary file
        self.load_config(config_file)

    def load_config(self, config_file):
        try:
            with open(config_file, 'r') as file:
                config = json.load(file)
            self.master_address = config['master_server']['address']
            self.master_port = config['master_server']['port']
            print(f"Configuration loaded from {config_file}")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading config file: {e}")
    
    def connect_to_master(self):
        attempts = 0
        socket_timeout = 5  # Timeout in seconds for each connection attempt
        while self.retry_attempts == -1 or attempts < self.retry_attempts:
            try:
                # Create a new socket for each attempt
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.client_socket.settimeout(socket_timeout)  # Set connection timeout
                print(f"Attempting to connect to Master Server at {self.master_address}:{self.master_port} (Attempt {attempts + 1})")
                # Attempt to connect to the master server
                self.client_socket.connect((self.master_address, self.master_port))
                print(f"Connected to Master Server at {self.master_address}:{self.master_port}")
                return  # Exit function after successful connection
            except Exception as e:
                print(f"Connection timed out after {socket_timeout} seconds. Retrying...")
                attempts += 1
                error_code = e.errno
                print(f"Error connecting to Master Server: {e}, Error Code: {error_code}")
                if self.retry_attempts != -1 and attempts >= self.retry_attempts:
                    print(f"Max retry attempts reached. Unable to connect to Master Server.")
                    return
                print(f"Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
        print("Failed to connect to the Master Server after all attempts.")


    def request_table(self, table_name):
        try:
            # Step 1: Construct a read request in JSON format
            request = json.dumps({"type": "read", "file_name": table_name})
            
            # Step 2: Send the read request to the Master Server
            self.client_socket.sendall(request.encode())
            print(f"Sent read request for file '{table_name}' to Master Server")

            # Step 3: Receive the chunk server address from the Master Server
            chunk_server_response = self.client_socket.recv(4096).decode().strip()
            
            # Check if the response is an error message
            if "Error" in chunk_server_response:
                print(f"Error from Master Server: {chunk_server_response}")
                return None  # Return None if there's an error response from the master server
            
            # Step 4: Parse the chunk server address
            chunk_server_address = chunk_server_response
            print(f"Received chunk server address from Master Server: {chunk_server_address}")

            # Ensure the chunk server address format is correct ('host:port')
            if ':' not in chunk_server_address:
                print(f"Invalid chunk server address format: {chunk_server_address}")
                return None  # Return None if the address format is invalid

            # Return the chunk server address after validation
            return chunk_server_address

        except Exception as e:
            print(f"An error occurred while requesting the file: {e}")
            return None  # Return None in case of any exception


    #reads data from table
    def retrieve_table_from_chunk_server(self, table_name, chunk_server_address, email):
        try:
            # Split the chunk server address into host and port
            host, port = chunk_server_address.split(':')
            chunk_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            chunk_socket.connect((host, int(port)))

            # Step 5: Send the table request to the Chunk Server
            request = json.dumps({"type": "readuser", "table_name": table_name, "email": email})
            chunk_socket.sendall(request.encode())
            print(f"Requested file '{table_name}' from Chunk Server at {chunk_server_address}")

            # Step 6: Receive the file data from the Chunk Server
            file_data = b''  # Initialize an empty byte string to hold all file data
            while True:
                chunk = chunk_socket.recv(4096)  # Receive file in chunks of 4096 bytes
                if not chunk:
                    print("No more data received from the Chunk Server.")
                    break  # End of file transfer
                file_data += chunk  # Append the received chunk to the file_data

            print(f"File successfully retrieved with {len(file_data)} bytes of data.")

            chunk_socket.close()

            # Return the file data
            return file_data

        except socket.error as e:
            print(f"Error retrieving file from Chunk Server: {e}")
            return None  # Return None in case of error


    #writes file to table
    def write_file(self, file_name, data):
            attempt = 0
            while attempt < self.retry_attempts:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as master_socket:
                        master_socket.connect((self.master_address, self.master_port))
                        write_request = {
                            "type": "write",
                            "file_name": file_name
                        }
                        master_socket.sendall(json.dumps(write_request).encode())
                        
                        primary_server_info = master_socket.recv(4096).decode()
                        primary_server = json.loads(primary_server_info)
                        primary_address, primary_port = primary_server["address"], primary_server["port"]
                        print(f"Received primary server address: {primary_address}:{primary_port}")

                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as primary_socket:
                        primary_socket.connect((primary_address, primary_port))
                        write_data = {
                            "type": "write",
                            "table_name": file_name,
                            "data": data,
                            "server": "primary"
                        }
                        primary_socket.sendall(json.dumps(write_data).encode())

                        response = primary_socket.recv(4096).decode()
                        if response == "Write success":
                            print("Write operation completed successfully.")
                            return True  # Write successful, exit function
                        else:
                            print("Primary chunk server failed to commit write. Retrying...")
                            attempt += 1
                            time.sleep(self.retry_delay)

                except Exception as e:
                    print(f"Error in write operation attempt {attempt + 1}: {e}")
                    attempt += 1
                    time.sleep(self.retry_delay)

            print("Write operation failed after all retry attempts.")
            return False

# Define a handler function with a while True loop
def client_handler():
    client = Client("mserver_config.json")
    
    while True:
        client.connect_to_master()
        
        choice = input("Enter 'read' to read a file or 'write' to write data to a file (or 'exit' to quit): ").strip().lower()
        if choice == 'exit':
            print("Exiting client.")
            break
        elif choice == 'read':
            file_name = input("Enter the file name you want to request: ").strip()
            client.request_file(file_name)
        elif choice == 'write':
            file_name = input("Enter the file name you want to write to: ").strip()


            id_value = input("Enter the id (or leave blank for auto-increment): ").strip()
            name = input("Enter the name: ").strip()
            email = input("Enter the email: ").strip()
            age = input("Enter the age: ").strip()

            # Create the data dictionary, omitting id if it's auto-incremented
            if id_value:
                data = {
                    "id": int(id_value),  # Ensure ID is an integer if provided
                    "name": name,
                    "email": email,
                    "age": int(age)  # Ensure age is an integer
                }
            else:
                data = {
                    "name": name,
                    "email": email,
                    "age": int(age)
                }


            client.write_file(file_name, data)


# Main function to call the handler
if __name__ == "__main__":
    client_handler()
