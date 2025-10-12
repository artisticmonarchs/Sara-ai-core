import requests
import concurrent.futures
import time

URL = "https://sara-ai-core-app.onrender.com/inference"
PAYLOAD = {"text": "Testing Sara AI under concurrent load."}
HEADERS = {"Content-Type": "application/json"}

def send_request(_):
    try:
        response = requests.post(URL, json=PAYLOAD, headers=HEADERS)
        return response.status_code
    except Exception as e:
        return f"Error: {e}"

def run_load_test(n, concurrency):
    print(f"Running {n} requests with concurrency={concurrency}")
    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        results = list(executor.map(send_request, range(n)))
    elapsed = time.time() - start
    success = results.count(200)
    print(f"✅ Completed {n} requests in {elapsed:.2f}s | Success: {success}/{n}")
    return results

if __name__ == "__main__":
    for n, c in [(10, 10), (50, 20), (100, 30)]:
        run_load_test(n, c)
