import httpx, re
URL = 'https://www.postgresql.org/message-id/dcea4840-18d0-4b5f-af16-1baefc563a3d%40proxel.se'
r = httpx.get(URL, follow_redirects=True, timeout=30)
print('status', r.status_code)
print('len', len(r.text))
# Look for header markers and pre/body content
for tag in ('From:', 'To:', 'Subject:', 'Date:', 'Message-Id:', 'In-Reply-To:', 'References:', '<pre'):
    idx = r.text.find(tag)
    print(tag, idx)
print('---first 5000 chars---')
print(r.text[:5000])
