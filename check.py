from kiteconnect import KiteConnect
from get_token import get_access_token

api_key = 'hwdft0qevt0p4vxb'
access_token1 = get_access_token()
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token1)
if __name__ == "__main__":
    try:
        profile = kite.profile()
        print("Profile Info:", profile)
    except Exception as e:
        print(f"Error: {e}")
