import redis

r = redis.Redis(host='red-d3ip3vodl3ps73dd24o0', port=6379, decode_responses=True)

# Test connection
print("Ping:", r.ping())

# Check persistence info
info = r.info('persistence')
print("Persistence info:", info)

# Optional quick key test
r.set('test_key', 'ok')
print("test_key:", r.get('test_key'))
r.delete('test_key')
