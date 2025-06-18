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
import subprocess
import logging

# Required libraries for aiohttp web server and websockets
# *** VIGTIGT: Flyttet tilbage til toppen ***
from aiohttp import web, ClientSession 

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S') # Add timestamp to logs
logger = logging.getLogger(__name__)

# --- Global variables for video data and synchronization ---
latest_video_chunk = None
new_chunk_event = asyncio.Event() 
ffmpeg_process = None

# Placeholder video path (optional, but good for robust startup)
PLACEHOLDER_WEBM_PATH = 'placeholder.webm'
PLACEHOLDER_WEBM_DATA = None

# RTSP/RTMP output configuration
# You NEED to change these to match your Motion Detection server's configuration!
# For RTST, it's typically a URL this server will "push" to or a URL Motion "pulls" from.
RTSP_OUTPUT_URL = "rtsp://127.0.0.1:8554/live" # EXAMPLE: Adjust if Motion creates an RTSP server
# OR for RTMP: RTMP_OUTPUT_URL = "rtmp://127.0.0.1/live/motionstream" # EXAMPLE: Adjust if Motion accepts RTMP push

# --- Load placeholder video at startup ---
try:
    if os.path.exists(PLACEHOLDER_WEBM_PATH):
        with open(PLACEHOLDER_WEBM_PATH, 'rb') as f:
            PLACEHOLDER_WEBM_DATA = f.read()
        logger.info(f"✅ Loaded placeholder WebM video from '{PLACEHOLDER_WEBM_PATH}'")
    else:
        logger.warning(f"⚠️ Could not find '{PLACEHOLDER_WEBM_PATH}'. FFmpeg might not start until first frame received.")
except Exception as e:
    logger.error(f"❌ Error loading placeholder WebM video: {e}")
    PLACEHOLDER_WEBM_DATA = None

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

# --- FFmpeg Management ---
async def start_ffmpeg_process(output_url):
    """
    Starts an FFmpeg subprocess to transcode WebM input (from stdin)
    to H.264 in an RTSP/RTMP stream.
    """
    global ffmpeg_process
    
    # Check if ffmpeg executable is available
    if not subprocess.run(['which', 'ffmpeg'], capture_output=True).returncode == 0:
        logger.error("❌ FFmpeg executable not found in system PATH!")
        logger.error("   Please install FFmpeg on your server. Here are common methods:")
        logger.error("   - Debian/Ubuntu: sudo apt-get update && sudo apt-get install ffmpeg")
        logger.error("   - Fedora: sudo dnf install ffmpeg")
        logger.error("   - CentOS/RHEL: sudo yum install epel-release && sudo yum install ffmpeg")
        logger.error("   - macOS (Homebrew): brew install ffmpeg")
        logger.error("   - Windows: Download from https://ffmpeg.org/download.html and add to PATH.")
        return # Do not proceed if FFmpeg is not found

    ffmpeg_cmd = [
        'ffmpeg',
        '-loglevel', 'warning', # Reduce FFmpeg output verbosity
        '-i', 'pipe:0',        # Read input from stdin
        '-c:v', 'libx264',
        '-preset', 'veryfast',
        '-tune', 'zerolatency',
        '-b:v', '2M',          # Adjust bitrate as needed (e.g., '1M', '3M')
        '-g', '30',            # Keyframe interval (important for stream recovery)
        '-f', 'rtsp',          # Output as RTSP stream
        '-rtsp_transport', 'tcp', # Ensure TCP transport for RTSP
        output_url
    ]
    
    logger.info(f"Starting FFmpeg with command: {' '.join(ffmpeg_cmd)}")
    try:
        ffmpeg_process = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, # Pipe stdout to capture errors/warnings if needed
            stderr=asyncio.subprocess.PIPE
        )
        logger.info(f"FFmpeg process started (PID: {ffmpeg_process.pid}). Pushing to {output_url}")

        # Start a task to read FFmpeg's stderr for debugging
        asyncio.create_task(read_ffmpeg_stderr())
        
    except FileNotFoundError: # This should ideally be caught by 'which ffmpeg' but as a fallback
        logger.error("❌ FFmpeg executable not found after attempting to start process. Check PATH.")
        ffmpeg_process = None
    except Exception as e:
        logger.error(f"❌ Failed to start FFmpeg process: {e}")
        ffmpeg_process = None

async def read_ffmpeg_stderr():
    """Reads FFmpeg's stderr to log any warnings or errors."""
    if ffmpeg_process and ffmpeg_process.stderr:
        while True:
            line = await ffmpeg_process.stderr.readline()
            if not line:
                break
            # Only log actual warnings/errors, not just info from ffmpeg
            if b"warning" in line.lower() or b"error" in line.lower() or b"failed" in line.lower():
                 logger.warning(f"FFmpeg STDERR: {line.decode().strip()}")
            else:
                 logger.debug(f"FFmpeg STDERR (debug): {line.decode().strip()}") # Use debug for less critical output
    logger.info("FFmpeg stderr reader stopped.")

async def stop_ffmpeg_process():
    """Stops the FFmpeg subprocess gracefully."""
    global ffmpeg_process
    if ffmpeg_process:
        logger.info("Stopping FFmpeg process...")
        try:
            ffmpeg_process.stdin.close() # Signal EOF to FFmpeg
            await ffmpeg_process.wait()  # Wait for FFmpeg to exit
            logger.info(f"FFmpeg process (PID: {ffmpeg_process.pid}) exited with code {ffmpeg_process.returncode}")
        except Exception as e:
            logger.error(f"Error stopping FFmpeg process: {e}")
        ffmpeg_process = None

# --- WebSocket handler for incoming camera streams ---
async def websocket_handler(request):
    """
    Handles incoming WebSocket connections from the camera (PWA).
    Receives WebM video chunks and feeds them to the FFmpeg process's stdin.
    """
    global latest_video_chunk 
    global new_chunk_event
    global ffmpeg_process

    ws = web.WebSocketResponse()
    await ws.prepare(request)
    logger.info("📡 Camera (PWA) connected via WebSocket")

    # Ensure FFmpeg is running when the first client connects
    if not ffmpeg_process or ffmpeg_process.returncode is not None:
        await start_ffmpeg_process(RTSP_OUTPUT_URL) # Use the configured output URL

    if not ffmpeg_process:
        logger.error("FFmpeg process is not running. Cannot receive video data.")
        await ws.close(code=1011, message="FFmpeg backend not available.")
        return ws

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.BINARY:
                video_chunk = msg.data
                # Feed the WebM chunk directly to FFmpeg's stdin
                if ffmpeg_process and ffmpeg_process.stdin and not ffmpeg_process.stdin.is_closing():
                    try:
                        ffmpeg_process.stdin.write(video_chunk)
                        await ffmpeg_process.stdin.drain() # Ensure data is written
                        new_chunk_event.set() 
                    except BrokenPipeError:
                        logger.error("FFmpeg stdin pipe is broken. FFmpeg likely crashed or exited.")
                        await ws.close(code=1011, message="FFmpeg pipe broken.")
                        break
                    except Exception as e:
                        logger.error(f"Error writing to FFmpeg stdin: {e}")
                        await ws.close(code=1011, message="Server error during video processing.")
                        break
                else:
                    logger.warning("Received video chunk but FFmpeg stdin not available or is closing. Dropping frame.")
                    # Optionally buffer or drop frames if FFmpeg isn't ready
                    latest_video_chunk = video_chunk # Keep the latest in case FFmpeg starts soon
            elif msg.type == web.WSMsgType.ERROR:
                logger.error(f"WS connection closed with exception: {ws.exception()}")
            elif msg.type == web.WSMsgType.CLOSE:
                logger.info("WS connection closed by client.")
                break # Exit loop if client closes connection gracefully

    except asyncio.CancelledError:
        logger.info("WebSocket handler task cancelled.")
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket handler: {e}")
    finally:
        logger.info("🔌 Camera (PWA) disconnected")
        await ws.close()
    return ws

# --- HTTP handler for MJPEG video stream (DEPRECATED/MODIFIED) ---
async def mjpeg_stream_handler(request):
    logger.warning("MJPEG stream endpoint accessed, but this server now streams RTSP/RTMP. This endpoint is deprecated and will return 404.")
    response = web.Response(text="MJPEG endpoint deprecated. Use RTSP/RTMP.", status=404)
    return response


# --- Main function to start the servers ---
async def main():
    """
    Main function that starts both the WebSocket and HTTP servers.
    Includes startup parameter validation and SSL context setup.
    """
    # --- Parameter Validation ---
    if len(sys.argv) != 3:
        logger.error("❌ Invalid number of arguments.")
        logger.error("Usage: python3 mediaserver.py <PWA_WebSocket_Port> <HTTP_Placeholder_Port>")
        logger.error("Example: python3 mediaserver.py 8181 8080")
        sys.exit(1)

    try:
        ws_port = int(sys.argv[1])
        http_port = int(sys.argv[2]) # HTTP port for potential future uses, or just a placeholder
    except ValueError:
        logger.error("❌ Port numbers must be integers.")
        logger.error("Usage: python3 mediaserver.py <PWA_WebSocket_Port> <HTTP_Placeholder_Port>")
        logger.error("Example: python3 mediaserver.py 8181 8080")
        sys.exit(1)

    # --- SSL Context Setup ---
    # Check for 'cryptography' library which is often needed for SSL features in Python
    try:
        import cryptography
    except ImportError:
        logger.critical("❌ Python 'cryptography' library not found!")
        logger.critical("   This library is essential for secure WebSocket (WSS) connections.")
        logger.critical("   Please install it: pip3 install cryptography")
        sys.exit(1)

    home_dir = os.path.expanduser("~")
    SSL_DIR = os.path.join(home_dir, ".ssl")
    CERT_FILE = os.path.join(SSL_DIR, "server.crt")
    KEY_FILE = os.path.join(SSL_DIR, "server.key")

    ssl_context = None
    try:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(CERT_FILE, KEY_FILE)
        logger.info(f"✅ SSL certificates loaded: {CERT_FILE}, {KEY_FILE}")
    except FileNotFoundError:
        logger.critical("❌ ERROR: SSL certificate or key files not found.")
        logger.critical(f"         Make sure '{CERT_FILE}' and '{KEY_FILE}' exist.")
        logger.critical("         The PWA will NOT be able to connect via HTTPS/WSS without these files.")
        logger.critical("         If you don't have them, you can generate self-signed certs.")
        sys.exit(1) # Exit if SSL files are missing, as WSS is critical
    except Exception as e:
        logger.critical(f"❌ ERROR: Could not load SSL certificates: {e}")
        logger.critical("         The PWA will NOT be able to connect via HTTPS/WSS.")
        logger.critical("         Check file permissions or certificate format.")
        sys.exit(1) # Exit on other SSL errors

    app = web.Application()
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("/", websocket_handler) # Fallback for clients connecting to root path

    app.router.add_get("/stream", mjpeg_stream_handler) 
    
    runner = web.AppRunner(app)
    await runner.setup()

    ws_site = web.TCPSite(runner, '0.0.0.0', ws_port, ssl_context=ssl_context)
    http_site = web.TCPSite(runner, '0.0.0.0', http_port) 

    # --- Print Server Status ---
    logger.info(f"✅ WebSocket server listening on: wss://{get_local_ip()}:{ws_port}/ws (for camera PWA)")
    logger.info(f"✅ FFmpeg will attempt to push RTSP/RTMP to: {RTSP_OUTPUT_URL}")
    logger.info(f"⚠️ Old MJPEG stream endpoint (http://{get_local_ip()}:{http_port}/stream) is now deprecated/removed and returns 404.")

    await ws_site.start()
    await http_site.start() 

    try:
        while True:
            await asyncio.sleep(3600) 
    except asyncio.CancelledError:
        pass 
    finally:
        logger.info("Server shutting down. Stopping FFmpeg...")
        await stop_ffmpeg_process() 
        await runner.cleanup() 

# --- Program entry point ---
if __name__ == "__main__":
    # --- Check for non-standard library dependencies (aiohttp) ---
    # This check needs to happen BEFORE aiohttp is actually used, 
    # but after the 'web' import has been done for module-level usage.
    # The actual import for usage is at the top. This is just for error messaging.
    try:
        import aiohttp
    except ImportError:
        logger.critical("❌ Missing required Python library: 'aiohttp'")
        logger.critical("   Please install it using pip3: pip3 install aiohttp")
        sys.exit(1)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nServer stopped by user (Ctrl+C).")
    except Exception as e:
        logger.critical(f"An unexpected error occurred during server execution: {e}", exc_info=True)
