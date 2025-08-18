from kiteconnect import KiteConnect

api_key = 'hwdft0qevt0p4vxb'
kite = KiteConnect(api_key=api_key)
data = kite.login_url()
print(data)
# print(kite.login_url())