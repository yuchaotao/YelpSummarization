import urllib2
import urllib
import json

url = "http://localhost:7474/db/data/transaction/commit"
request = urllib2.Request(url)
request.add_header("Content-Type", "application/json")
request.add_header("Accept-Charset", "utf-8")
request.add_header("Accept", "application/json")
data = """
{
  "statements" : [ {
    "statement" : "CREATE (n) RETURN id(n)"
  } ]
}
"""
post_data = data
request.add_data(post_data)
res = urllib2.urlopen(request)
msg = res.read()
msg = json.loads(msg)
for k,v in msg.items():
  print(k,v)