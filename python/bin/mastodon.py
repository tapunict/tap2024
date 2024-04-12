import httpx
import asyncio
import json
import sys
import os

async def run(hashtag,fp):
    client = httpx.AsyncClient()
    async with client.stream(
        "GET", "https://mastodon.uno/api/v1/streaming/hashtag/local?tag="+hashtag, timeout=None
    ) as response:
        event_type = None
        async for line in response.aiter_lines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("event:"):
                event_type = line.split(":", 1)[1].strip()
                continue
            if line.startswith("data:"):
                data = line.split(":", 1)[1]
                decoded = json.loads(data)
                if not isinstance(decoded, dict):
                    print("event_type", event_type, "data", data, " (not a dict)")
                    continue
                print(data)
                #print(json.dumps(decoded, indent=2))
                # print data to fp
                written=fp.write(data+"\n")
                print("written",written)
                fp.flush() 
                continue

if __name__ == "__main__":
    hashtag = os.getenv("hashtag", "tap")
    outputdir = os.getenv("outputdir", ".")
    outputfile = outputdir+"/"+hashtag+".jsonl"
    fp = open(outputfile, "a")
    asyncio.run(run(hashtag,fp))
    ## Close file for writing
    fp.close()