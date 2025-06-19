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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S') # Add timestamp to logs
logger = logging.getLogger(__name__)

# --- Global variables for video data and synchronization ---
latest_video_chunk = None
new_chunk_event = asyncio.Event() 
ffmpeg_process = None
client_video_mime_type = None # Global variabel til at gemme klientens MIME type

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
    Starts an FFmpeg subprocess to transcode input (from stdin)
    to H.264 in an RTSP/RTMP stream.
    The input format is determined by `client_video_mime_type`.
    """
    global ffmpeg_process

    # Determine FFmpeg input format based on client's MIME type
    ffmpeg_input_format = None
    # 'video/mp4;codecs=avc1' -> 'mp4'
    # 'video/webm;codecs=vp8' -> 'webm'
    if client_video_mime_type:
        if 'mp4' in client_video_mime_type:
            ffmpeg_input_format = 'mp4'
        elif 'webm' in client_video_mime_type:
            ffmpeg_input_format = 'webm'
        else:
            logger.error(f"❌ Ukendt eller ikke-understøttet klient MIME type: {client_video_mime_type}. Kan ikke starte FFmpeg.")
            return # Kan ikke starte FFmpeg uden at kende inputformatet
    else:
        logger.error("❌ client_video_mime_type er ikke indstillet. Kan ikke starte FFmpeg.")
        return # Kan ikke starte FFmpeg uden at kende inputformatet

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
        '-f', ffmpeg_input_format, # Explicitly specify input format
        '-i', 'pipe:0',          # Read input from stdin
        '-c:v', 'libx264',
        '-preset', 'veryfast',
        '-tune', 'zerolatency',
        '-b:v', '2M',            # Adjust bitrate as needed (e.g., '1M', '3M')
        '-g', '30',              # Keyframe interval (important for stream recovery)
        '-f', 'rtsp',            # Output as RTSP stream
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
    Receives WebM/MP4 video chunks and feeds them to the FFmpeg process's stdin.
    """
    global latest_video_chunk 
    global new_chunk_event
    global ffmpeg_process
    global client_video_mime_type # Brug denne globale variabel

    ws = web.WebSocketResponse()
    await ws.prepare(request)
    logger.info("📡 Camera (PWA) connected via WebSocket")

    # --- Trin 1: Modtag den indledende MIME type besked fra klienten ---
    # Denne loop er specifikt for den første besked, som skal være JSON
    async for msg in ws:
        if msg.type == web.WSMsgType.TEXT:
            try:
                data = json.loads(msg.data)
                if data.get("type") == "init" and data.get("mimeType"):
                    client_video_mime_type = data["mimeType"]
                    logger.info(f"Received client MIME type: {client_video_mime_type}")
                    break # Afslut denne loop, vi fik init beskeden
            except json.JSONDecodeError:
                logger.warning(f"Received non-JSON text message: {msg.data}. Expected init message.")
                await ws.close(code=1003) # FJERNET 'reason' argument
                return ws
        else:
            logger.warning(f"Received non-text initial message type: {msg.type}. Expected JSON MIME type.")
            await ws.close(code=1003) # FJERNET 'reason' argument
            return ws

    if not client_video_mime_type:
        logger.error("Klient sendte ikke forventet MIME type. Lukker forbindelse.")
        await ws.close(code=1003) # FJERNET 'reason' argument
        return ws

    # Sørg for, at FFmpeg kører, når den første klient forbinder OG vi har mime-typen
    # Vi starter FFmpeg her, da vi nu har inputformatet
    if not ffmpeg_process or ffmpeg_process.returncode is not None:
        await start_ffmpeg_process(RTSP_OUTPUT_URL) # Brug den konfigurerede output URL

    if not ffmpeg_process:
        logger.error("FFmpeg processen kører ikke. Kan ikke modtage video data.")
        await ws.close(code=1011) # FJERNET 'reason' argument
        return ws

    try:
        async for msg in ws: # Denne loop behandler efterfølgende binære video-chunks
            if msg.type == web.WSMsgType.BINARY:
                video_chunk = msg.data
                # Før WebM/MP4 chunken direkte til FFmpeg's stdin
                if ffmpeg_process and ffmpeg_process.stdin and not ffmpeg_process.stdin.is_closing():
                    try:
                        ffmpeg_process.stdin.write(video_chunk)
                        await ffmpeg_process.stdin.drain() # Sørg for, at data er skrevet
                        new_chunk_event.set()
                    except BrokenPipeError:
                        logger.error("FFmpeg stdin pipe er brudt. FFmpeg er sandsynligvis styrtet ned eller afsluttet.")
                        await ws.close(code=1011) # FJERNET 'reason' argument
                        break
                    except Exception as e:
                        logger.error(f"Fejl ved skrivning til FFmpeg stdin: {e}")
                        await ws.close(code=1011) # FJERNET 'reason' argument
                        break
                else:
                    logger.warning("Modtog video chunk, men FFmpeg stdin er ikke tilgængelig eller lukker. Dropper frame.")
                    # Valgfrit: buffer eller drop frames, hvis FFmpeg ikke er klar
                    latest_video_chunk = video_chunk # Behold den seneste i tilfælde af at FFmpeg starter snart
            elif msg.type == web.WSMsgType.ERROR:
                logger.error(f"WS forbindelse lukket med undtagelse: {ws.exception()}")
            elif msg.type == web.WSMsgType.CLOSE:
                logger.info("WS forbindelse lukket af klient.")
                break # Afslut loop, hvis klienten lukker forbindelse elegant
            elif msg.type == web.WSMsgType.TEXT: # Håndter uventede tekstbeskeder efter init
                logger.warning(f"Modtog uventet tekstbesked efter init: {msg.data}")

    except asyncio.CancelledError:
        logger.info("WebSocket handler task annulleret.")
    except Exception as e:
        logger.error(f"Uventet fejl i WebSocket handler: {e}")
    finally:
        logger.info("🔌 Camera (PWA) disconnected")
        # Stop IKKE FFmpeg her, da andre klienter måske vil forbinde.
        # FFmpeg skal kun stoppe, når serveren lukker ned.
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
        logger.error("❌ Ugyldigt antal argumenter.")
        logger.error("Anvendelse: python3 mediaserver.py <PWA_WebSocket_Port> <HTTP_Placeholder_Port>")
        logger.error("Eksempel: python3 mediaserver.py 8181 8080")
        sys.exit(1)

    try:
        ws_port = int(sys.argv[1])
        http_port = int(sys.argv[2]) # HTTP port for potential future uses, or just a placeholder
    except ValueError:
        logger.error("❌ Portnumre skal være heltal.")
        logger.error("Anvendelse: python3 mediaserver.py <PWA_WebSocket_Port> <HTTP_Placeholder_Port>")
        logger.error("Eksempel: python3 mediaserver.py 8181 8080")
        sys.exit(1)

    # --- SSL Context Setup ---
    # Check for 'cryptography' library which is often needed for SSL features in Python
    try:
        import cryptography
    except ImportError:
        logger.critical("❌ Python 'cryptography' bibliotek ikke fundet!")
        logger.critical("   Dette bibliotek er essentielt for sikre WebSocket (WSS) forbindelser.")
        logger.critical("   Installer det venligst: pip3 install cryptography")
        sys.exit(1)

    home_dir = os.path.expanduser("~")
    SSL_DIR = os.path.join(home_dir, ".ssl")
    CERT_FILE = os.path.join(SSL_DIR, "server.crt")
    KEY_FILE = os.path.join(SSL_DIR, "server.key")

    ssl_context = None
    try:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(CERT_FILE, KEY_FILE)
        logger.info(f"✅ SSL certifikater indlæst: {CERT_FILE}, {KEY_FILE}")
    except FileNotFoundError:
        logger.critical("❌ FEJL: SSL certifikat- eller nøglefiler ikke fundet.")
        logger.critical(f"         Sørg for, at '{CERT_FILE}' og '{KEY_FILE}' eksisterer.")
        logger.critical("         PWA'en vil IKKE kunne forbinde via HTTPS/WSS uden disse filer.")
        sys.exit(1) # Afslut, hvis SSL-filer mangler, da WSS er kritisk
    except Exception as e:
        logger.critical(f"❌ FEJL: Kunne ikke indlæse SSL certifikater: {e}")
        logger.critical("         PWA'en vil IKKE kunne forbinde via HTTPS/WSS.")
        logger.critical("         Kontroller filtilladelser eller certifikatformat.")
        sys.exit(1) # Afslut på andre SSL-fejl

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
        logger.info("Server lukker ned. Stopper FFmpeg...")
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
        logger.critical("❌ Mangler påkrævet Python bibliotek: 'aiohttp'")
        logger.critical("   Installer det venligst med pip3: pip3 install aiohttp")
        sys.exit(1)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nServer stoppet af bruger (Ctrl+C).")
    except Exception as e:
        logger.critical(f"En uventet fejl opstod under serverudførelse: {e}", exc_info=True)
