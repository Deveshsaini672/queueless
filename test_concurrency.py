import asyncio
import httpx

COUNTER_ID = "stress_test"
NUM_REQUESTS = 100

async def join_once(client: httpx.AsyncClient):
    resp = await client.post(f"http://127.0.0.1:8000/queue/{COUNTER_ID}/join")
    return resp.json()["token"]

async def main():
    async with httpx.AsyncClient() as client:
        tasks = [join_once(client) for _ in range(NUM_REQUESTS)]
        tokens = await asyncio.gather(*tasks)

    print(f"Got {len(tokens)} tokens")
    print(f"Unique tokens: {len(set(tokens))}")

    if len(tokens) != len(set(tokens)):
        duplicates = [t for t in tokens if tokens.count(t) > 1]
        print(f"DUPLICATES FOUND: {set(duplicates)}")
    else:
        print(" No duplicates — Redis INCR held up under concurrency")

asyncio.run(main())