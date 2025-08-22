from kiteconnect import KiteConnect
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse as urlparse
import webbrowser
import json
import os
import threading
from datetime import date

# ---------------- CONFIG ---------------- #
API_KEY = "hwdft0qevt0p4vxb"
API_SECRET = "yr3j7vunt6rjm1zo16lb1uucyypebc55"
REDIRECT_URI = "http://localhost:8000"
PORT = 8000
TOKEN_FILE = "kite_access_token.json"
# ----------------------------------------- #

kite = KiteConnect(api_key=API_KEY)

class KiteHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        query = urlparse.urlparse(self.path).query
        params = urlparse.parse_qs(query)
        request_token = params.get("request_token", [None])[0]

        if request_token:
            try:
                data = kite.generate_session(request_token, api_secret=API_SECRET)
                access_token = data["access_token"]

                kite.set_access_token(access_token)

                # Save token with today's date
                with open(TOKEN_FILE, "w") as f:
                    json.dump({
                        "access_token": access_token,
                        "date": str(date.today())
                    }, f)

                self.server.access_token = access_token

            except Exception as e:
                print("❌ Error generating access token:", e)

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Login successful! You can close this window.")

        threading.Thread(target=self.server.shutdown).start()

def start_server():
    httpd = HTTPServer(("localhost", PORT), KiteHandler)
    httpd.access_token = None
    httpd.serve_forever()
    return httpd.access_token

def load_token():
    """Load saved token if still valid."""
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            token_data = json.load(f)

        if token_data.get("date") == str(date.today()):
            access_token = token_data.get("access_token")
            if access_token:
                kite.set_access_token(access_token)
                try:
                    kite.profile()  # test if token works
                    return access_token
                except:
                    pass
    return None

def get_access_token():
    """Main function to get a valid access token."""
    token = load_token()
    if token:
        print(f"✅ Loaded existing token: {token}")
        return token

    # Need new token
    login_url = kite.login_url()
    print(f"🔗 Opening Kite login URL: {login_url}")
    webbrowser.open(login_url)

    print("⏳ Waiting for login and redirect...")
    server_thread = threading.Thread(target=start_server)
    server_thread.daemon = True
    server_thread.start()
    server_thread.join()

    # After server shuts down, try loading the new token
    return load_token()

# ---------------- Example Usage ---------------- #
if __name__ == "__main__":
    access_token = get_access_token()
    # print("🎯 Access Token in variable:", access_token)
