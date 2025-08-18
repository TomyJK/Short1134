from kiteconnect import KiteConnect
import os
import sys
import webbrowser

# --------------------------
# CONFIG
# --------------------------
API_KEY = "hwdft0qevt0p4vxb"
API_SECRET = "yr3j7vunt6rjm1zo16lb1uucyypebc55"
TOKEN_FILE = "access_token.txt"  # Saved in current project folder

kite = KiteConnect(api_key=API_KEY)

def save_access_token(token):
    with open(TOKEN_FILE, "w") as f:
        f.write(token)

def load_access_token():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            return f.read().strip()
    return None

def generate_new_token():
    # Step 1: Print login URL
    print("\nLogin URL (open in browser):\n", kite.login_url(), "\n")
    login_url = kite.login_url()
    webbrowser.open(login_url)

    # Step 2: Get request token from user
    request_token = input("Paste the request token here: ").strip()

    if not request_token:
        print("❌ No request token entered. Exiting.")
        sys.exit(1)

    try:
        # Step 3: Generate access token
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        access_token = data["access_token"]
        kite.set_access_token(access_token)

        # Step 4: Validate by checking profile
        profile = kite.profile()
        print("✅ Access token generated successfully for:", profile["user_name"])

        # Step 5: Save token
        save_access_token(access_token)
        print(f"💾 Access token saved to {TOKEN_FILE}")
        print(f"Access Token: {access_token}")

    except Exception as e:
        print("❌ ",e, " Try again:")
        sys.exit(1)

def main():
    token = load_access_token()
    if token:
        kite.set_access_token(token)
        try:
            # Validate token
            profile = kite.profile()
            print("✅ Loaded existing access token for:", profile["user_name"])
            print(f"Access Token: {token}")
            return
        except Exception as e:
            print("⚠️ Stored token invalid/expired. Need to generate a new one.")

    generate_new_token()

if __name__ == "__main__":
    main()
