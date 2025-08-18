import os
import sys

from kiteconnect import KiteConnect

API_KEY = "hwdft0qevt0p4vxb"
API_SECRET = "yr3j7vunt6rjm1zo16lb1uucyypebc55"

def get_access_token():
    """
    Reads the saved access token from file.
    Validates it with KiteConnect.
    Returns the token as a string (access_token1).
    Raises FileNotFoundError if file missing.
    Raises Exception if token invalid.
    """
    TOKEN_FILE = "access_token.txt"
    if not os.path.exists(TOKEN_FILE):
        print(f"⚠️ Token file '{TOKEN_FILE}' not found. Please generate it first.")
        sys.exit(0)

    with open(TOKEN_FILE, "r") as f:
        access_token1 = f.read().strip()

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(access_token1)

    try:
        # Test token validity
        profile = kite.profile()
        print(f"✅ Access token valid for: {profile['user_name']}")
        # print(f"Access Token: {access_token1}")
    except Exception as e:
        print("❌ Access token is invalid or expired. Regenerate access token.")
        sys.exit(0)
    return access_token1

# a_token = get_access_token()
# print(f"Access Token: {a_token}")
