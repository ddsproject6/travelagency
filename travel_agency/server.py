import socket
import json
import os
import threading
import os
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, select, text


class Server:
    def __init__(self, server_id):
        self.server_id = server_id
        self.files = {}
        self.server_address = None  # Placeholder for server IP
        self.server_port = None     # Placeholder for server port
        self.load_server_files()

    def load_server_files(self):
        # Load files from the corresponding directory based on server_id
        directory = f"{self.server_id}_files"  # E.g., "server1_files"
        
        # Load files from files_metadata.json
        with open("files_metadata.json", "r") as f:
            file_metadata = json.load(f)

        # For each file in the metadata, check if this server manages it
        for file_name, file_info in file_metadata.items():
            if self.server_id in file_info["replicas"]:
                # File path is inside the server's specific directory
                db_file = f"{self.server_id}.db"  # E.g., "server1.db"
                db_file_new = f"test.db"
                file_path_new = os.path.join(directory, db_file_new)
                file_path = os.path.join(directory, db_file)
                if os.path.exists(file_path_new):
                    self.files[file_name] = file_path_new
        
        print(f"Server {self.server_id} managing tables: {list(self.files.keys())}")

    def start(self):
        try:
            # Create the server socket
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind((self.server_address, self.server_port))  # Corrected the attribute names
            server_socket.listen(5)
            print(f"Server {self.server_id} listening on {self.server_address}:{self.server_port}")  # Corrected print statement

            # Accept clients in a loop
            while True:
                client_socket, addr = server_socket.accept()
                print(f"Connection from {addr}")
                client_handler = threading.Thread(target=self.handle_client, args=(client_socket,))
                client_handler.start()

        except Exception as e:
            print(f"Error starting server {self.server_id}: {e}")  # Corrected the attribute name
        finally:
            server_socket.close()
    
    def handle_client(self, client_socket):
        try:
            # Step 1: Receive and decode the request from the client
            data = client_socket.recv(4096).decode().strip()
            request = json.loads(data)  # Parse JSON request
            request_type = request.get("type")
            file_name = request.get("table_name")
            email = request.get("email") 

            # Handle write requests differently based on the server type
            if request_type == "write":
                content = request.get("data")
                server_type = request.get("server")  # 'primary' or 'secondary'
                print(f"Received write request for {file_name} with content '{content}'")

                # # Primary server handles write and forwards to secondary servers
                # if server_type == "primary":
                #     print(f"Primary server {self.server_id} handling write to {file_name} with content '{content}'")

                #     # Append data to the file locally
                #     success = self.append_to_file(file_name, content)

                #     if success:
                #         # Deduce secondaries by excluding the current server from the replicas
                #         secondaries = self.get_secondaries(file_name)
                #         print(f"Secondaries for {file_name}: {secondaries}")

                #         # Forward the write to secondaries
                #         secondary_status = []
                #         for secondary_address in secondaries:
                #             try:
                #                 sec_host, sec_port = secondary_address
                #                 secondary_status.append(self.forward_to_secondary(sec_host, sec_port, file_name, content))
                #             except Exception as e:
                #                 print(f"Error forwarding to secondary {secondary_address}: {e}")
                #                 secondary_status.append(False)

                #         # Verify if all secondaries succeeded
                #         if all(secondary_status):
                #             client_socket.sendall(b"Write success")
                #             print("Write committed successfully to primary and all secondaries")
                #         else:
                #             # Rollback if any secondary fails
                #             self.rollback_file(file_name)
                #             for sec_addr in secondaries:
                #                 self.rollback_secondary(sec_addr, file_name)
                #             client_socket.sendall(b"Write failed")
                #             print("Write failed; rolled back changes")
                #     else:
                #         client_socket.sendall(b"Write failed")
                #         print("Write failed locally")

                if server_type == "primary":
                    print(f"Primary server {self.server_id} handling write to {file_name} with content '{content}'")

                    # Append data to the database table locally with manual commit
                    success = self.append_to_file(file_name, content)

                    if success:
                        # Deduce secondaries by excluding the current server from the replicas
                        secondaries = self.get_secondaries(file_name)
                        print(f"Secondaries for {file_name}: {secondaries}")

                        # Forward the write to secondaries
                        secondary_status = []
                        for secondary_address in secondaries:
                            try:
                                sec_host, sec_port = secondary_address
                                secondary_status.append(self.forward_to_secondary(sec_host, sec_port, file_name, content))
                            except Exception as e:
                                print(f"Error forwarding to secondary {secondary_address}: {e}")
                                secondary_status.append(False)

                        # Verify if all secondaries succeeded
                        if all(secondary_status):
                            client_socket.sendall(b"Write success")
                            print("Write committed successfully to primary and all secondaries")
                        else:
                            # Rollback if any secondary fails
                            self.rollback_file(file_name)
                            for sec_addr in secondaries:
                                self.rollback_secondary(sec_addr, file_name)
                            client_socket.sendall(b"Write failed")
                            print("Write failed; rolled back changes")
                    else:
                        client_socket.sendall(b"Write failed")
                        print("Write failed locally")




                # Secondary server forwards the write to its file (it does not initiate writes)
                elif server_type == "secondary":
                    print(f"Secondary server {self.server_id} received write request for {file_name} with content '{content}'")
                    success = self.append_to_file(file_name, content)
                    if success:
                        client_socket.sendall(b"Write success")
                        print(f"Write successful on secondary server {self.server_id}")
                    else:
                        client_socket.sendall(b"Write failed")
                        print(f"Write failed on secondary server {self.server_id}")

            # Handle read requests (not server_type dependent)
            # if request_type == "read":
            #     print(f"Client requested to read: {file_name}")
            #     if file_name in self.files and os.path.exists(self.files[file_name]):
            #         # Open and send the file contents in chunks
            #         try:
            #             with open(self.files[file_name], 'rb') as f:
            #                 while chunk := f.read(4096):
            #                     client_socket.sendall(chunk)
            #             print(f"Sent file {file_name} to client")
            #         except Exception as e:
            #             print(f"Error reading file {file_name}: {e}")
            #             client_socket.sendall(f"Error: Could not read file {file_name}".encode())
            #     else:
            #         error_message = f"Error: File {file_name} not found on server."
            #         print(error_message)
            #         client_socket.sendall(error_message.encode())


            # if request_type == "read":
            #     print(f"Client requested to read table: {file_name}")
                
            #     # Check if the file path exists and is in the server's files
            #     if file_name in self.files and os.path.exists(self.files[file_name]):
            #         # Database file path
            #         db_file_path = self.files[file_name]
                    
            #         try:
            #             # Create an SQLAlchemy engine to connect to the SQLite database
            #             engine = create_engine(f'sqlite:///{db_file_path}')
            #             connection = engine.connect()
            #             metadata = MetaData()

            #             # Reflect the table (assuming the table name is the same as `file_name`)
            #             table = Table(file_name, metadata, autoload_with=engine)

            #             # Perform a SELECT * query
            #             query = select([table])
            #             result = connection.execute(query)

            #             # Send the result rows to the client
            #             for row in result:
            #                 # Convert each row to a string and send to the client
            #                 client_socket.sendall(f"{row}\n".encode())
                        
            #             print(f"Sent table {file_name} content to client")

            #         except Exception as e:
            #             print(f"Error accessing table {file_name}: {e}")
            #             client_socket.sendall(f"Error: Could not read table {file_name}".encode())

            #         finally:
            #             # Close the connection
            #             connection.close()

            #     else:
            #         error_message = f"Error: Database file {file_name}.db not found on server."
            #         print(error_message)
            #         client_socket.sendall(error_message.encode())

            if request_type == "read":
                print(f"Client requested to read table: {file_name}")
                
                # Check if the file path exists and is in the server's files
                if file_name in self.files and os.path.exists(self.files[file_name]):
                    # Database file path
                    db_file_path = self.files[file_name]
                    
                    try:
                        # Step 1: Create an SQLAlchemy engine to connect to the SQLite database
                        engine = create_engine(f'sqlite:///{db_file_path}', echo=True)

                        # Step 2: Connect to the database
                        with engine.connect() as connection:
                            
                            # Step 3: Execute a raw SQL query using text() to wrap the SQL query
                            query = text(f"SELECT * FROM {file_name} WHERE email = '{email}' LIMIT 1")
                            result = connection.execute(query)

                            
                            # Step 4: Fetch and send the results to the client
                            for row in result:
                                # Convert each row to a string and send it to the client
                                row_data = ", ".join([str(value) for value in row])  # Join row data as a string
                                client_socket.sendall(f"{row_data}\n".encode())
                            
                            print(f"Sent table {file_name} content to client")
                    
                    except Exception as e:
                        print(f"Error accessing table {file_name}: {e}")
                        client_socket.sendall(f"Error: Could not read table {file_name}".encode())

                else:
                    error_message = f"Error: Database file {file_name}. not found on server."
                    print(error_message)
                    client_socket.sendall(error_message.encode())

            if request_type == "readuser":
                print(f"Client requested to read table: {file_name}")
                
                # Check if the file path exists and is in the server's files
                if file_name in self.files and os.path.exists(self.files[file_name]):
                    # Database file path
                    db_file_path = self.files[file_name]
                    
                    try:
                        # Step 1: Create an SQLAlchemy engine to connect to the SQLite database
                        engine = create_engine(f'sqlite:///{db_file_path}', echo=True)

                        # Step 2: Connect to the database
                        with engine.connect() as connection:
                            
                            # Step 3: Execute a raw SQL query using text() to wrap the SQL query
                            query = text(f"SELECT * FROM {file_name} WHERE email = '{email}' LIMIT 1")

                            # Execute the query
                            result = connection.execute(query)

                            # Fetch the first result (assuming you're using LIMIT 1)
                            row = result.fetchone()  # This will fetch one row (or None if no results)

                            if row:
                                # Convert the row into a dictionary (assuming SQLAlchemy result is iterable)
                                row_data = {column: value for column, value in zip(result.keys(), row)}

                                # Convert the dictionary to a JSON string
                                result_json = json.dumps(row_data)

                                # Send the result to the client as a JSON string (including newline for separation)
                                client_socket.sendall(f"{result_json}\n".encode())
                            else:
                                # If no result is found, send a message indicating that
                                error_message = json.dumps({"error": f"No data found for email: {email}"})
                                client_socket.sendall(f"{error_message}\n".encode())
                            # # Step 4: Fetch and send the results to the client
                            # for row in result:
                            #     # Convert each row to a string and send it to the client
                            #     row_data = ", ".join([str(value) for value in row])  # Join row data as a string
                            #     client_socket.sendall(f"{row_data}\n".encode())
                            
                            print(f"Sent table {file_name} content to client")
                    
                    except Exception as e:
                        print(f"Error accessing table {file_name}: {e}")
                        client_socket.sendall(f"Error: Could not read table {file_name}".encode())

                else:
                    error_message = f"Error: Database file {file_name}. not found on server."
                    print(error_message)
                    client_socket.sendall(error_message.encode())


    
            else:
                print(f"Unknown request type {request_type}")


            

            

        except Exception as e:
            print(f"Error handling client request: {e}")
        finally:
            client_socket.close()


    # def append_to_file(self, file_name, content):
    #     try:
    #         file_path = os.path.join(f"{self.server_id}_files", file_name)
    #         # Open the file in append mode
    #         with open(file_path, 'a') as f:
    #             f.write(content)
    #         print(f"Write successful on {file_name}")
    #         return True
    #     except Exception as e:
    #         print(f"Error writing to {file_name}: {e}")
    #         return False
    def append_to_file(self, file_name, content):
        """
        Insert data into the SQLite database file using raw SQL.
        The table name is derived from `file_name`, and the DB file path is retrieved from `self.files[file_name]`.
        The transaction is manually committed or rolled back based on the result.
        """
        # Get the database file path from self.files
        db_file_path = self.files[file_name]

        # Connect to the database using SQLAlchemy's create_engine
        engine = create_engine(f"sqlite:///{db_file_path}")

        # Extract data from content (which is assumed to be a dictionary)
        id_value = content.get("id")  # This will be None if it's auto-incremented
        name = content.get("name")
        email = content.get("email")
        age = content.get("age")

        # Create the raw SQL insert statement without the `id` field for auto-increment
        sql = f"""
        INSERT INTO {file_name} (name, email, age)
        VALUES (:name, :email, :age)
        """

        # If `id` is provided, modify the query to include it
        if id_value:
            sql = f"""
            INSERT INTO {file_name} (id, name, email, age)
            VALUES (:id, :name, :email, :age)
            """

        connection = None
        transaction = None

        try:
            # Connect to the database
            connection = engine.connect()

            # Begin a transaction
            transaction = connection.begin()

            # Execute the SQL insert
            connection.execute(text(sql), {"id": id_value, "name": name, "email": email, "age": age})

            # Commit the transaction manually
            transaction.commit()
            print(f"Data successfully inserted into the table '{file_name}' in the database '{db_file_path}'")
            return True
        except Exception as e:
            # Rollback the transaction if an error occurs
            if transaction:
                transaction.rollback()
            print(f"Error inserting data into the table '{file_name}' in the database '{db_file_path}': {e}")
            return False
        finally:
            # Close the connection
            if connection:
                connection.close()


    def get_secondaries(self, file_name):
        try:
            # Load file metadata
            with open("files_metadata.json", "r") as f:
                file_metadata = json.load(f)

            if file_name in file_metadata:
                replicas = file_metadata[file_name]["replicas"]
                # Exclude the current server from the list of replicas
                secondaries = [replica for replica in replicas if replica != self.server_id]
                
                # Retrieve full server address (host, port) from servers.json for each secondary server
                secondary_addresses = []
                with open("servers.json", "r") as servers_file:
                    servers_data = json.load(servers_file)
                    
                    # Get the actual address (IP and port) for each secondary server
                    for secondary in secondaries:
                        if secondary in servers_data:
                            secondary_addresses.append(tuple(servers_data[secondary]))
                        else:
                            print(f"Warning: No address found for {secondary} in servers.json")

                return secondary_addresses
            else:
                print(f"Error: {file_name} not found in metadata")
                return []
        except Exception as e:
            print(f"Error getting secondaries: {e}")
            return []

    def forward_to_secondary(self, host, port, file_name, content):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sec_sock:
                sec_sock.connect((host, port))
                write_data = {
                    "type": "write",
                    "file_name": file_name,
                    "data": content,
                    "server": "secondary"
                }
                sec_sock.sendall(json.dumps(write_data).encode())

                response = sec_sock.recv(4096).decode()
                return response == "Write success"      
        except Exception as e:
            print(f"Error communicating with secondary: {e}")
            return False

    def rollback_file(self, file_name):
        # Logic to rollback changes locally (e.g., restore from a backup or clear)
        print(f"Rolling back changes to {file_name} on this server")

    def rollback_secondary(self, sec_addr, file_name):
        try:
            sec_host, sec_port = sec_addr
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sec_sock:
                sec_sock.connect((sec_host, int(sec_port)))
                rollback_request = json.dumps({"type": "rollback", "file_name": file_name})
                sec_sock.sendall(rollback_request.encode())
        except Exception as e:
            print(f"Error rolling back on secondary {sec_addr}: {e}")


def start_server_thread(server_id, server_address, server_port):
    server = Server(server_id)
    server.server_address = server_address  # Set the correct server address
    server.server_port = server_port        # Set the correct server port
    server.start()

if __name__ == "__main__":
    # Load the server configuration from server_config.json
    with open("mserver_config.json", "r") as config_file:
        config_data = json.load(config_file)
    
    # Get the list of chunk servers
    chunk_servers = config_data["chunk_servers"]
    
    # Start each server in its own thread
    threads = []
    for server_info in chunk_servers:
        server_id = server_info["name"]
        server_address = server_info["address"]
        server_port = server_info["port"]

        # Create and start a thread for each server
        thread = threading.Thread(target=start_server_thread, args=(server_id, server_address, server_port))
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete (optional)
    for thread in threads:
        thread.join()
