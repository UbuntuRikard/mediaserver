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
import json # Nyt import for JSON-parsing

# Required libraries for aiohttp web server and websockets
from aiohttp import web, ClientSession 

# --- Logging Setup ---
# Standard logfilnavn og niveau, hvis ingen konfigurationsfil findes
DEFAULT_LOG_FILE_NAME = "mediaserver.log"
DEFAULT_LOG_LEVEL = "DEBUG" # Vi beholder DEBUG for nu for at få detaljerede FFmpeg logs

CONFIG_FILE_NAME = "mediaserver.json" # Navnet på konfigurationsfilen

def setup_logging():
    """
    Sætter logning op baseret på mediaserver.json eller standardindstillinger.
    Returnerer den konfigurerede logger-instans.
    """
    log_file_path = None
    log_level = DEFAULT_LOG_LEVEL

    script_dir = os.path.dirname(os.path.abspath(__file__)) # Mappen hvor scriptet kører fra
    config_file_full_path = os.path.join(script_dir, CONFIG_FILE_NAME)

    # Forsøg at indlæse konfiguration fra fil
    if os.path.exists(config_file_full_path):
        try:
            with open(config_file_full_path, 'r') as f:
                config = json.load(f)
                configured_log_file_path = config.get("log_file_path")
                configured_log_level = config.get("log_level")

                if configured_log_file_path:
                    # Hvis konfigureret sti er relativ, gør den relativ til scriptets mappe
                    if not os.path.isabs(configured_log_file_path):
                        log_file_path = os.path.join(script_dir, configured_log_file_path)
                    else:
                        log_file_path = configured_log_file_path
                else:
                    log_file_path = os.path.join(script_dir, DEFAULT_LOG_FILE_NAME) # Standard til script-mappe

                if configured_log_level:
                    log_level = configured_log_level.upper()
                
                print(f"Indlæste logkonfiguration fra '{CONFIG_FILE_NAME}'. Logfil: '{log_file_path}', Logniveau: '{log_level}'")

        except Exception as e:
            # Hvis konfigurationsfilen ikke kan læses, brug standardindstillinger
            print(f"Advarsel: Kunne ikke læse eller parse '{CONFIG_FILE_NAME}': {e}. Bruger standardlogning til fil i script-mappen.")
            log_file_path = os.path.join(script_dir, DEFAULT_LOG_FILE_NAME)
    else:
        # Hvis konfigurationsfilen ikke findes, brug standardindstillinger
        log_file_path = os.path.join(script_dir, DEFAULT_LOG_FILE_NAME)
        print(f"Ingen '{CONFIG_FILE_NAME}' fundet. Bruger standardlogning til fil i script-mappen: '{log_file_path}'")

    # Sørg for, at mappen til logfilen eksisterer
    log_dir = os.path.dirname(log_file_path)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
            print(f"Oprettet logmappe: {log_dir}")
        except OSError as e:
            print(f"Fejl ved oprettelse af logmappe '{log_dir}': {e}. Logning kan mislykkes.")
            # Fallback til nuværende arbejdsmappe, hvis oprettelse af mappe mislykkes
            log_file_path = DEFAULT_LOG_FILE_NAME # Relativ til CWD hvis mkdir fejler
            print(f"Falder tilbage til logning i nuværende arbejdsmappe: '{os.path.join(os.getcwd(), log_file_path)}'")


    # Konfigurer root loggeren
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Ryd eksisterende handlere (nyttigt ved genstart i interaktive miljøer)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Tilføj en filhandler
    file_handler = logging.FileHandler(log_file_path)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Returner logger-instansen
    return root_logger

# Kald setup_logging ved scriptets start
logger = setup_logging() 

# --- Global variables for video data and synchronization ---
latest_video_chunk = None
new_chunk_event = asyncio.Event() 
ffmpeg_process = None
client_video_mime_type = None 

# Placeholder video path (optional, but good for robust startup)
PLACEHOLDER_WEBM_PATH = 'placeholder.webm'
PLACEHOLDER_WEBM_DATA = None

# RTSP/RTMP output configuration
RTSP_OUTPUT_URL = "rtsp://127.0.0.1:8554/live" 

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
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip

# --- FFmpeg Management ---
async def start_ffmpeg_process(output_url):
    """
    Starts an FFmpeg subprocess to transcode input (from stdin)
    to H.264 in an RTSP/RTMP stream.
    The input format is determined by `client_video_mime_type`.
    """
    global ffmpeg_process

    ffmpeg_input_format = None
    if client_video_mime_type:
        if 'mp4' in client_video_mime_type:
            ffmpeg_input_format = 'mp4'
        elif 'webm' in client_video_mime_type:
            ffmpeg_input_format = 'webm'
        else:
            logger.error(f"❌ Ukendt eller ikke-understøttet klient MIME type: {client_video_mime_type}. Kan ikke starte FFmpeg.")
            return 
    else:
        logger.error("❌ client_video_mime_type er ikke indstillet. Kan ikke starte FFmpeg.")
        return 

    if not subprocess.run(['which', 'ffmpeg'], capture_output=True).returncode == 0:
        logger.error("❌ FFmpeg executable not found in system PATH!")
        logger.error("   Please install FFmpeg on your server. Here are common methods:")
        logger.error("   - Debian/Ubuntu: sudo apt-get update && sudo apt-get install ffmpeg")
        logger.error("   - Fedora: sudo dnf install ffmpeg")
        logger.error("   - CentOS/RHEL: sudo yum install epel-release && sudo yum install ffmpeg")
        logger.error("   - macOS (Homebrew): brew install ffmpeg")
        logger.error("   - Windows: Download from https://ffmpeg.org/download.html and add to PATH.")
        return 

    ffmpeg_cmd = [
        'ffmpeg',
        '-loglevel', 'debug', # Set FFmpeg's loglevel to debug
        '-f', ffmpeg_input_format, 
        '-i', 'pipe:0',          
        '-probesize', '32',      # Help FFmpeg detect stream properties
        '-analyzeduration', '0', # Faster analysis, but might miss complex streams
        '-c:v', 'libx264',
        '-preset', 'veryfast',
        '-tune', 'zerolatency',
        '-b:v', '2M',            
        '-g', '30',              
        '-f', 'rtsp',            
        '-rtsp_transport', 'tcp', 
        output_url
    ]
    
    logger.info(f"Starting FFmpeg with command: {' '.join(ffmpeg_cmd)}")
    try:
        ffmpeg_process = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, 
            stderr=asyncio.subprocess.PIPE
        )
        logger.info(f"FFmpeg process started (PID: {ffmpeg_process.pid}). Pushing to {output_url}")

        asyncio.create_task(read_ffmpeg_stderr())
        
    except FileNotFoundError: 
        logger.error("❌ FFmpeg executable not found after attempting to start process. Check PATH.")
        ffmpeg_process = None
    except Exception as e:
        logger.error(f"❌ Failed to start FFmpeg process: {e}")
        ffmpeg_process = None

async def read_ffmpeg_stderr():
    """Reads FFmpeg's stderr to log any warnings or errors."""
    if ffmpeg_process and ffmpeg_process.stderr:
        while True:
            try:
                # Readline with a short timeout to prevent blocking indefinitely
                line = await asyncio.wait_for(ffmpeg_process.stderr.readline(), timeout=0.5) 
                if not line:
                    # EOF, process has likely exited
                    break
                logger.debug(f"FFmpeg STDERR: {line.decode(errors='ignore').strip()}") 
            except asyncio.TimeoutError:
                # No new line for 0.5s, continue trying
                await asyncio.sleep(0.1) # Small sleep to yield control
            except Exception as e:
                logger.error(f"Error reading FFmpeg stderr: {e}")
                break
    logger.info("FFmpeg stderr reader stopped.")

async def stop_ffmpeg_process():
    """Stops the FFmpeg subprocess gracefully and logs exit details."""
    global ffmpeg_process
    if ffmpeg_process:
        logger.info("Stopping FFmpeg process...")
        try:
            # Ensure stdin is closed to signal EOF to FFmpeg
            if ffmpeg_process.stdin and not ffmpeg_process.stdin.is_closing():
                ffmpeg_process.stdin.close()
                await ffmpeg_process.stdin.wait_closed() # Wait for stdin to fully close

            # Wait for FFmpeg to exit and get its return code
            returncode = await asyncio.wait_for(ffmpeg_process.wait(), timeout=5) # Wait up to 5 seconds
            
            logger.info(f"FFmpeg process (PID: {ffmpeg_process.pid}) exited with code {returncode}")

            # Read any remaining stderr output after FFmpeg has exited
            if ffmpeg_process.stderr:
                remaining_stderr = await ffmpeg_process.stderr.read()
                if remaining_stderr:
                    logger.error(f"FFmpeg STDERR (remaining after exit): {remaining_stderr.decode(errors='ignore').strip()}")

        except asyncio.TimeoutError:
            logger.error(f"❌ FFmpeg process (PID: {ffmpeg_process.pid}) did not exit gracefully within timeout. Terminating.")
            ffmpeg_process.terminate()
            await ffmpeg_process.wait()
            logger.error(f"FFmpeg process terminated. Return code: {ffmpeg_process.returncode}")
        except Exception as e:
            logger.error(f"Error stopping FFmpeg process: {e}")
        finally:
            ffmpeg_process = None

# --- WebSocket handler for incoming camera streams ---
async def websocket_handler(request):
    """
    Handles incoming WebSocket connections from the camera (PWA).
    Receives WebM/MP4 video chunks and feeds them to the FFmpeg process's stdin.
    """
    global latest_video_chunk 
    global new_chunk_event
    global ffmpeg_process
    global client_video_mime_type 

    ws = web.WebSocketResponse()
    await ws.prepare(request)
    logger.info("📡 Camera (PWA) connected via WebSocket")

    async for msg in ws:
        if msg.type == web.WSMsgType.TEXT:
            try:
                data = json.loads(msg.data)
                if data.get("type") == "init" and data.get("mimeType"):
                    client_video_mime_type = data["mimeType"]
                    logger.info(f"Received client MIME type: {client_video_mime_type}")
                    break 
            except json.JSONDecodeError:
                logger.warning(f"Received non-JSON text message: {msg.data}. Expected init message.")
                await ws.close(code=1003) 
                return ws
        else:
            logger.warning(f"Received non-text initial message type: {msg.type}. Expected JSON MIME type.")
            await ws.close(code=1003) 
            return ws

    if not client_video_mime_type:
        logger.error("Client did not send expected MIME type. Closing connection.")
        await ws.close(code=1003) 
        return ws

    if not ffmpeg_process or ffmpeg_process.returncode is not None:
        await start_ffmpeg_process(RTSP_OUTPUT_URL) 

    if not ffmpeg_process:
        logger.error("FFmpeg process is not running. Cannot receive video data.")
        await ws.close(code=1011) 
        return ws

    try:
        async for msg in ws: 
            if msg.type == web.WSMsgType.BINARY:
                video_chunk = msg.data
                if ffmpeg_process and ffmpeg_process.stdin and not ffmpeg_process.stdin.is_closing():
                    try:
                        ffmpeg_process.stdin.write(video_chunk)
                        await asyncio.wait_for(ffmpeg_process.stdin.drain(), timeout=5) # Add timeout to drain
                        new_chunk_event.set()
                    except BrokenPipeError:
                        logger.error("FFmpeg stdin pipe is broken. FFmpeg likely crashed or exited.")
                        await ws.close(code=1011) 
                        break
                    except asyncio.TimeoutError:
                        logger.error("Timeout while writing to FFmpeg stdin. FFmpeg might be unresponsive.")
                        await ws.close(code=1011)
                        break
                    except Exception as e:
                        logger.error(f"Error writing to FFmpeg stdin: {e}")
                        await ws.close(code=1011) 
                        break
                else:
                    logger.warning("Received video chunk but FFmpeg stdin not available or is closing. Dropping frame.")
            elif msg.type == web.WSMsgType.ERROR:
                logger.error(f"WS connection closed with exception: {ws.exception()}")
            elif msg.type == web.WSMsgType.CLOSE:
                logger.info("WS connection closed by client.")
                break 
            elif msg.type == web.WSMsgType.TEXT: 
                logger.warning(f"Received unexpected text message after init: {msg.data}")

    except asyncio.CancelledError:
        logger.info("WebSocket handler task cancelled.")
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket handler: {e}")
    finally:
        logger.info("🔌 Camera (PWA) disconnected")
        # Do NOT stop FFmpeg here, as other clients might connect.
        # FFmpeg should only stop when the server shuts down.
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
    if len(sys.argv) != 3:
        logger.error("❌ Invalid number of arguments.")
        logger.error("Usage: python3 mediaserver.py <PWA_WebSocket_Port> <HTTP_Placeholder_Port>")
        logger.error("Example: python3 mediaserver.py 8181 8080")
        sys.exit(1)

    try:
        ws_port = int(sys.argv[1])
        http_port = int(sys.argv[2]) 
    except ValueError:
        logger.error("❌ Port numbers must be integers.")
        logger.error("Usage: python3 mediaserver.py <PWA_WebSocket_Port> <HTTP_Placeholder_Port>")
        logger.error("Example: python3 mediaserver.py 8181 8080")
        sys.exit(1)

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
        sys.exit(1) 
    except Exception as e:
        logger.critical(f"❌ ERROR: Could not load SSL certificates: {e}")
        logger.critical("         The PWA will NOT be able to connect via HTTPS/WSS.")
        logger.critical("         Check file permissions or certificate format.")
        sys.exit(1) 

    app = web.Application()
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("/", websocket_handler) 

    app.router.add_get("/stream", mjpeg_stream_handler) 
    
    runner = web.AppRunner(app)
    await runner.setup()

    ws_site = web.TCPSite(runner, '0.0.0.0', ws_port, ssl_context=ssl_context)
    http_site = web.TCPSite(runner, '0.0.0.0', http_port) 

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
