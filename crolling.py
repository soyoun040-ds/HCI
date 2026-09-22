import requests, json

url = "https://apis.naver.com/blogfe/cafe-add-api/external/v1/aib/ai-pick/contributors"
headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://mate.naver.com/"}

# count를 크게, topicIds를 다 넣어보기
topics = ",".join(f"TOPIC_{i:03d}" for i in range(1, 41))
r = requests.get(url, params={"topicIds": topics, "count": 100}, headers=headers)

data = r.json()
print(r.status_code)
print(json.dumps(data, ensure_ascii=False)[:500])
