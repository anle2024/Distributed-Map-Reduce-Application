"""
RPC utilities for distributed MapReduce communication between coordinator and workers.
"""

import socket
import threading
import json
import time
from typing import Any, Dict, Optional, Callable


class RPCServer:
    """Simple RPC server for coordinator to handle worker requests."""

    def __init__(self, host: str = "localhost", port: int = 0):
        self.host = host
        self.port = port
        self.socket = None
        self.running = False
        self.handlers: Dict[str, Callable] = {}

    def register_handler(self, method_name: str, handler: Callable):
        """Register a method handler for RPC calls."""
        self.handlers[method_name] = handler

    def start(self) -> int:
        """Start the RPC server and return the port it's listening on."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))
        self.socket.listen(5)

        # Get the actual port if port was 0
        actual_port = self.socket.getsockname()[1]
        self.port = actual_port

        self.running = True

        # Start server thread
        server_thread = threading.Thread(target=self._server_loop, daemon=True)
        server_thread.start()

        return actual_port

    def stop(self):
        """Stop the RPC server."""
        self.running = False
        if self.socket:
            self.socket.close()

    def _server_loop(self):
        """Main server loop to handle incoming connections."""
        while self.running:
            try:
                client_socket, addr = self.socket.accept()
                client_thread = threading.Thread(
                    target=self._handle_client, args=(client_socket,), daemon=True
                )
                client_thread.start()
            except OSError:
                # Socket was closed
                break

    def _handle_client(self, client_socket: socket.socket):
        """Handle a single client connection."""
        try:
            # Read request
            data = b""
            while True:
                chunk = client_socket.recv(1024)
                if not chunk:
                    break
                data += chunk
                if b"\n" in data:
                    break

            if not data:
                return

            # Parse request
            request_str = data.decode("utf-8").strip()
            request = json.loads(request_str)

            method = request.get("method")
            params = request.get("params", {})

            # Call handler
            if method in self.handlers:
                try:
                    result = self.handlers[method](**params)
                    response = {"success": True, "result": result}
                except Exception as e:
                    response = {"success": False, "error": str(e)}
            else:
                response = {"success": False, "error": f"Unknown method: {method}"}

            # Send response
            response_str = json.dumps(response) + "\n"
            client_socket.send(response_str.encode("utf-8"))

        except Exception as e:
            error_response = {"success": False, "error": str(e)}
            response_str = json.dumps(error_response) + "\n"
            try:
                client_socket.send(response_str.encode("utf-8"))
            except:
                pass
        finally:
            client_socket.close()


class RPCClient:
    """Simple RPC client for workers to communicate with coordinator."""

    def __init__(self, host: str = "localhost", port: int = None):
        self.host = host
        self.port = port

    def call(self, method: str, **params) -> Optional[Any]:
        """Make an RPC call to the server."""
        if self.port is None:
            return None

        try:
            # Create socket connection
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(5.0)  # 5 second timeout
            client_socket.connect((self.host, self.port))

            # Send request
            request = {"method": method, "params": params}
            request_str = json.dumps(request) + "\n"
            client_socket.send(request_str.encode("utf-8"))

            # Read response
            data = b""
            while True:
                chunk = client_socket.recv(1024)
                if not chunk:
                    break
                data += chunk
                if b"\n" in data:
                    break

            if not data:
                return None

            # Parse response
            response_str = data.decode("utf-8").strip()
            response = json.loads(response_str)

            client_socket.close()

            if response.get("success"):
                return response.get("result")
            else:
                print(f"RPC error: {response.get('error')}")
                return None

        except Exception as e:
            print(f"RPC call failed: {e}")
            return None


def create_coordinator_socket_file() -> str:
    """Create a socket file path for the coordinator."""
    import tempfile
    import os

    return os.path.join(tempfile.gettempdir(), f"mr-{os.getpid()}")


def cleanup_socket_file(socket_path: str):
    """Clean up socket file."""
    import os

    try:
        os.unlink(socket_path)
    except OSError:
        pass
