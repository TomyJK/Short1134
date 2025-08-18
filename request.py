from kiteconnect import KiteConnect

api_key = 'hwdft0qevt0p4vxb'
api_secret = 'yr3j7vunt6rjm1zo16lb1uucyypebc55'
request_token = ''

kite = KiteConnect(api_key=api_key)
data = kite.generate_session(request_token, api_secret)
access_token = data["access_token"]

# Set the access token
kite.set_access_token(access_token)

print("Access Token:", access_token)