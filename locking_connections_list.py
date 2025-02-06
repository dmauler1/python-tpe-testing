from threading import Lock
from concurrent.futures import ThreadPoolExecutor
import time
import random

class Connection:
    def __init__(self, host, id):
        self.host = host
        self.id = id

    def __enter__(self):
        print('Connecting...')
        return self
    
    def disconnect(self):
        print('Disconnecting...')

    def get_id(self):
        return self.id

    
class Connections:
    def __init__(self):
        self._connections = []
        self.lock = Lock()

    def get_connection(self):
        with self.lock:
            return self._connections.pop()

    def put_connection(self, connection):
        with self.lock:            
            self._connections.append(connection)

    def is_connection_available(self):
        print(f"Checking for available connections {len(self._connections)}")
        with self.lock:
            return len(self._connections) > 0
        
    def shutdown_connections(self):
        with self.lock:
            for connection in self._connections:
                connection.disconnect()

class FTPTask:
    def __init__(self, job_id, connection_pool):
        self.job_id = job_id
        self.connection_pool = connection_pool

    def run(self):
        while not self.connection_pool.is_connection_available():
            print(f"Waiting for connection for job {self.job_id}")
            time.sleep(5)
        else:
            print(f"Running job {self.job_id}")
            connection = self.connection_pool.get_connection() 
            print(f"Got connection {connection.get_id()} for job {self.job_id}")  
            time.sleep(random.randint(5, 6))   
            return connection
              
class Main:
    def __init__(self):
        self._connections = Connections()        
        self._futures = []

    def _init_connections(self):
        for i in range(5):
            connection = Connection(f'host{i}', i)
            self._connections.put_connection(connection)

    def _run_task(self, job_id, connectionPool):
        ftpTask = FTPTask(id, connectionPool)
        return ftpTask.run()

    def run(self):
        try:
            self._init_connections()
            # This will load up the tpe's internal queue and move on to the futures loop
            # This provides more granular control over the futures and whats going on inside them
            tpe = ThreadPoolExecutor(max_workers=2)
            job_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            #job_ids = [1]

            for id in job_ids:
                print(f"Submitting job id {id}")                
                self._futures.append(tpe.submit(self._run_task, id, self._connections))

            print("outside of loop")
            
            try: 
                print(f"Checking futures {len(self._futures)}")       
                while self._futures:
                    for future in self._futures:
                        if future.done():
                            print("Future loop")
                            connection = future.result()
                            
                            print(f"Putting connection {connection.get_id()} back on connection pool")                            
                            self._connections.put_connection(connection)

                            self._futures.remove(future)
                            print(f"{len(self._futures)} futures left")

                            time.sleep(1)
            finally:                
                tpe.shutdown(wait=False, cancel_futures=True) # Throw the brakes, rely on restart behavior

        finally:
            if tpe:
                tpe.shutdown(wait=True)
                print("Shutdown complete")

            if self._connections:
                self._connections.shutdown_connections()
                print("Connections shutdown complete")

if __name__ == '__main__':
    main = Main()
    main.run()




    