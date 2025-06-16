#!/usr/bin/env python3
###
# This file is part of webcam App
# Copyright (C) 2025 Rikard Svenningsen
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# # MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.nu/licenses/>.
####
import asyncio
import socket
import sys
import ssl
import os
# Required libraries for aiohttp web server and websockets
from aiohttp import web, ClientSession
import websockets

# --- Global variables for image data and synchronization ---
# Initialize latest_frame with a static placeholder image.
# This prevents the MJPEG stream from sending empty data when Motion connects
# but the PWA has not yet sent a frame.
# Ensure you have a small 'placeholder.jpg' file in the same directory as mediaserver.py.
PLACEHOLDER_JPEG_PATH = 'placeholder.jpg'
latest_frame = None # Will be updated with placeholder or first actual frame
new_frame_event = asyncio.Event()

# --- Load or create a placeholder image at startup ---
try:
    if os.path.exists(PLACEHOLDER_JPEG_PATH):
        with open(PLACEHOLDER_JPEG_PATH, 'rb') as f:
            latest_frame = f.read()
        print(f"✅ Loaded placeholder image from '{PLACEHOLDER_JPEG_PATH}'")
    else:
        # If the file isn't found, create a very simple black JPEG (minimal size)
        # This is a basic 8x8 pixel black JPEG image.
        latest_frame = b'\xFF\xD8\xFF\xE0\x00\x10\x4A\x46\x49\x46\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xFF\xDB\x00\x43\x00\x03\x02\x02\x02\x02\x02\x03\x02\x02\x02\x03\x03\x03\x03\x04\x03\x03\x04\x04\x04\x04\x05\x05\x05\x05\x06\x06\x06\x06\x07\x07\x07\x07\x08\x08\x08\x08\x09\x09\x09\x09\x0A\x0A\x0A\x0A\x0B\x0B\x0B\x0B\xFF\xC0\x00\x11\x08\x00\x08\x00\x08\x01\x01\x11\x00\xFF\xC4\x00\x1F\x00\x00\x01\x05\x01\x01\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\xFF\xC4\x00\x1F\x01\x00\x03\x01\x01\x01\x01\x01\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\xFF\xDA\x00\x0C\x03\x01\x00\x02\x11\x03\x11\x00\x3F\x00\xF5\xFC\x03\xFF\xD9'
        print("⚠️ Could not find 'placeholder.jpg'. Using a simple black fallback image.")
except Exception as e:
    print(f"❌ Error loading/creating placeholder image: {e}")
    # If all fails, ensure latest_frame is None to prevent sending invalid data
    latest_frame = None

# --- Helper function to find local IP address ---
def get_local_ip():
    """
    Finds the local IP address of the machine.
    Used to display which addresses the servers are listening on.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        # Fallback to localhost if no network connection
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip

# --- WebSocket handler for incoming camera streams ---
async def websocket_handler(request):
    """
    Handles incoming WebSocket connections from the camera (PWA).
    Receives image data and forwards it to the MJPEG stream.
    """
    global latest_frame
    global new_frame_event

    ws = web.WebSocketResponse()
    await ws.prepare(request)
    print("📡 Camera (PWA) connected via WebSocket")

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.BINARY:
                frame_data = msg.data
                latest_frame = frame_data # Update the latest frame for the MJPEG stream
                new_frame_event.set()     # Signal to the MJPEG handler that a new frame is ready
            elif msg.type == web.WSMsgType.ERROR:
                print(f"WS connection closed with exception: {ws.exception()}")

    except asyncio.CancelledError:
        print("WebSocket handler task cancelled.")
    except Exception as e:
        print(f"Unexpected error in WebSocket handler: {e}")
    finally:
        print("🔌 Camera (PWA) disconnected")
        await ws.close()
    return ws

# --- HTTP handler for MJPEG video stream ---
async def mjpeg_stream_handler(request):
    """
    Handles HTTP requests for the MJPEG video stream.
    Sends images to the client as they become available.
    """
    global latest_frame
    global new_frame_event

    response = web.StreamResponse()
    response.content_type = "multipart/x-mixed-replace; boundary=frame"
    
    # These headers are for the *overall stream response*, not per image.
    # 'identity' Transfer-Encoding should prevent chunking for the top-level response.
    response.headers['Transfer-Encoding'] = 'identity' 
    response.headers['Connection'] = 'keep-alive' 

    await response.prepare(request)

    print("🎥 MJPEG stream client connected")

    try:
        while True:
            # Wait for a new frame, but continue sending the latest frame
            # even if no new one has arrived for a while, to keep the stream alive.
            # Timeout of 5 seconds means we send the same frame every 5 seconds
            # if no new frame is provided by the PWA.
            try:
                await asyncio.wait_for(new_frame_event.wait(), timeout=5.0)
                new_frame_event.clear()
            except asyncio.TimeoutError:
                # If timeout, continue to send the latest frame (which is still in latest_frame)
                pass

            if latest_frame:
                try:
                    # **IMPORTANT CHANGE HERE:** Add Content-Length for each JPEG part
                    await response.write(b"--frame\r\n")
                    await response.write(b"Content-Type: image/jpeg\r\n")
                    await response.write(f"Content-Length: {len(latest_frame)}\r\n".encode()) # <-- NEW LINE
                    await response.write(b"\r\n") # End of headers for this part
                    await response.write(latest_frame)
                    await response.write(b"\r\n") # End of image data

                except ConnectionResetError:
                    print("❌ MJPEG stream client disconnected (connection reset by peer)")
                    break
                except Exception as e:
                    print(f"Error writing to MJPEG stream: {e}. Client likely disconnected.")
                    break
            else:
                # If latest_frame is None (e.g., if placeholder failed and no PWA data yet)
                # Wait a bit before trying again to avoid spamming
                print("⚠️ No frame available to send via MJPEG stream. Waiting...")
                await asyncio.sleep(0.5)

            # Small pause to avoid unnecessary CPU usage if many frames arrive quickly
            # This sleep should be adjusted depending on the desired framerate for output.
            await asyncio.sleep(0.001)

    except asyncio.CancelledError:
        print("🚫 MJPEG stream task cancelled (e.g., server shutting down).")
    except Exception as e:
        print(f"Unexpected error in MJPEG stream handler: {e}")
    finally:
        print("🚫 MJPEG stream client disconnected.")
    return response

# --- Main function to start the servers ---
async def main():
    """
    Main function that starts both the WebSocket and HTTP servers.
    Includes startup parameter validation and SSL context setup.
    """
    # --- Parameter Validation ---
    if len(sys.argv) != 3:
        print("❌ Invalid number of arguments.")
        print("Usage: python3 mediaserver.py <PWA_WebSocket_Port> <MJPEG_Streaming_Port>")
        print("Example: python3 mediaserver.py 8181 8080")
        sys.exit(1)

    try:
        ws_port = int(sys.argv[1])
        http_port = int(sys.argv[2])
    except ValueError:
        print("❌ Port numbers must be integers.")
        print("Usage: python3 mediaserver.py <PWA_WebSocket_Port> <MJPEG_Streaming_Port>")
        print("Example: python3 mediaserver.py 8181 8080")
        sys.exit(1)

    # --- SSL Context Setup ---
    # Construct the full paths to your certificate and key files based on your home directory.
    # This matches the path used by your existing https.server script.
    home_dir = os.path.expanduser("~")
    SSL_DIR = os.path.join(home_dir, ".ssl")
    CERT_FILE = os.path.join(SSL_DIR, "server.crt")
    KEY_FILE = os.path.join(SSL_DIR, "server.key")

    ssl_context = None
    try:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(CERT_FILE, KEY_FILE)
        print(f"✅ SSL certificates loaded: {CERT_FILE}, {KEY_FILE}")
    except FileNotFoundError:
        print("❌ ERROR: SSL certificate or key files not found.")
        print(f"          Make sure '{CERT_FILE}' and '{KEY_FILE}' exist.")
        print("          The PWA will NOT be able to connect via HTTPS without these files.")
    except Exception as e:
        print(f"❌ ERROR: Could not load SSL certificates: {e}")
        print("          The PWA will NOT be able to connect via HTTPS.")

    app = web.Application()
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("/", websocket_handler) # Fallback for clients connecting to root path

    app.router.add_get("/stream", mjpeg_stream_handler) # MJPEG stream endpoint

    runner = web.AppRunner(app)
    await runner.setup()

    # Use '0.0.0.0' to listen on all available network interfaces.
    # This is important for your smartphone to connect.
    ws_site = web.TCPSite(runner, '0.0.0.0', ws_port, ssl_context=ssl_context)
    # MJPEG stream remains HTTP.
    http_site = web.TCPSite(runner, '0.0.0.0', http_port)

    # --- Print Server Status ---
    # WebSocket now uses wss://
    print(f"✅ WebSocket server listening on: wss://{get_local_ip()}:{ws_port}/ws and wss://{get_local_ip()}:{ws_port}/ (for camera)")
    # MJPEG remains http://, forced to localhost for clarity for Motion
    print(f"✅ MJPEG stream available for Motion on: http://127.0.0.1:{http_port}/stream")

    await ws_site.start()
    await http_site.start()

    try:
        # Keeps the main task alive indefinitely so the servers can run.
        while True:
            await asyncio.sleep(3600) # Sleep for an hour at a time
    except asyncio.CancelledError:
        pass # Ignore cancellation (e.g., on program exit).
    finally:
        await runner.cleanup() # Clean up aiohttp runner resources

# --- Program entry point ---
if __name__ == "__main__":
    # --- Check for non-standard library dependencies ---
    try:
        # These are re-imported here to provide a clear error message
        # if they are missing before the main program logic runs.
        import aiohttp
        import websockets
    except ImportError:
        print("❌ Missing required Python libraries.")
        print("   Please install them using pip3:")
        print("   pip3 install aiohttp websockets")
        sys.exit(1)

    # This specific check for ClientConnectorError is maintained for robustness
    # even though the main aiohttp import generally covers it.
    try:
        from aiohttp.client_exceptions import ClientConnectorError
    except ImportError:
        print("❌ Could not import aiohttp.client_exceptions.ClientConnectorError.")
        print("   This might indicate an issue with your aiohttp installation.")
        print("   Please try reinstalling aiohttp: pip3 install --upgrade aiohttp")
        sys.exit(1)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped by user (Ctrl+C).")
    except Exception as e:
        print(f"An unexpected error occurred during server execution: {e}")
