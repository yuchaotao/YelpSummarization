# coding:utf8
"""
    self.session.run("CREATE (a:Person {name: {name}, title: {title}})",
                {"name": "Arthur", "title": "King"})

    result = self.session.run("MATCH (a:Person) WHERE a.name = {name} "
                         "RETURN a.name AS name, a.title AS title",
                         {"name": "Arthur"})
      for record in result:
        print("%s %s" % (record["title"], record["name"]))
"""
import json 
import time



class Neo4jHelper(object):

  def __init__(self):
    from neo4j.v1 import GraphDatabase, basic_auth
    self.driver = GraphDatabase.driver("bolt://localhost:7787", auth=basic_auth("neo4j", "neo4j"))
    self.run_cnt = 0
    self.run_buffer = []

  def __del__(self):
    if self.run_buffer:
      session = self.driver.session()
      for run in self.run_buffer:
        res = session.run(*run)
      self.run_buffer = []
      session.close() 
    self.driver.close()

  def batch_run(self, *s):
    self.run_cnt += 1
    self.run_buffer.append(s)
    if self.run_cnt % 100 == 0:
      print(self.run_cnt)
      session = self.driver.session()
      for run in self.run_buffer:
        res = session.run(*run)
      self.run_buffer = []
      session.close()   

  def run(self, *s):
    session = self.driver.session()
    res = session.run(*s)
    session.close()
    return res

def test():
  db = Neo4jHelper()
  print db.run("CREATE (a:Person {name: {name}, title: {title}})",
               {"name": "Arthur", "title": "King"})
    
def create_graph():
  start = time.time()
  db = Neo4jHelper()
  # db.run("MATCH (n) DETACH DELETE n")
  tagged_review_file = open("../data/tagged_review.json", "r")
  line_cnt = 0
  total_line_cnt = 726121 # partial
  for line in tagged_review_file:
    line_cnt += 1
    data = json.loads(line)
    rid = data['review_id']
    bid = data['business_id']
    sid = 0
    for sentence in data['text']:
      sentence = sentence.encode('unicode-escape')
      new_words = []
      pid = 0
      for tagged_word in sentence.split(' '):
        slash = tagged_word.rfind("/")
        word = tagged_word[:slash]
        pos = tagged_word[slash+1:]
        new_words.append([word, pos, pid])
        pid += 1
      query = '''
        CREATE (s:sid {{v:{sid}}})
        MERGE (r:rid {{v:'{rid}'}})
        MERGE (b:bid {{v:'{bid}'}})
        MERGE (s)-[:sid2rid]->(r)
        MERGE (r)-[:rid2bid]->(b)
        WITH {new_words} AS new_words, s
        UNWIND range(0, size(new_words)-2) AS i
        MERGE (w1:word {{s:new_words[i][0]}})
        MERGE (w2:word {{s:new_words[i+1][0]}})
        MERGE (w1)-[:next]-> (w2)
        MERGE (w1)-[:word2sid {{pos: new_words[i][1], pid: new_words[i][2]}}]->(s)
        MERGE (w2)-[:word2sid {{pos: new_words[i+1][1], pid: new_words[i+1][2]}}]->(s)
      '''.format(sid=sid, rid=rid, bid=bid, new_words=str(new_words))
      # print(query)
      # exit(0)
      res = db.batch_run(query)
      sid += 1
    if line_cnt == 10: exit(0)
    if line_cnt % 100 == 0:
      print(line_cnt, "%f%%"%(line_cnt*1.0/total_line_cnt*100), "%fmin"%((time.time()-start)/60))
      start = time.time()

def create_summary(bid_list=None):
  SIGMA_VSN = 5
  bid_list = ['uYHaNptLzDLoV_JZ_MuzUA'] # TODO: increase bid_list with one more bid
  # Retrieve all nodes related to bid_list
  query = '''
  MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
  WHERE (:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})<-[*2]-()<-[:word2sid]-(w1)
  AND (:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})<-[*2]-()<-[:word2sid]-(w2)
  AND (:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})<-[*2]-()<-[:word2sid]-(w3)
  RETURN DISTINCT reduce(x='', cha in extract(n in nodes(p)|n.s) | x+cha+' ') AS sent
  LIMIT 10
  '''
  db = Neo4jHelper()
  res = db.run(query)
  print([x.values() for x in res.records()])
  exit(0)

  # Compute average pid 
  query = '''
  MATCH (word:word)-[word2sid:word2sid]->(sid:sid)-[*2]->(:bid {{v:'{bid}'}})
  WITH word, word2sid, sid LIMIT 100
  WITH word.s AS word_s, AVG(toFloat(word2sid.pid)) AS avg_pid, COUNT(word) AS cnt
  WHERE avg_pid < {SIGMA_VSN}
  RETURN word_s, avg_pid
  '''.format(bid=bid_list[0], SIGMA_VSN=SIGMA_VSN)
  print query

def test_create_summary():
  db = Neo4jHelper()
  query = '''
  MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
,(w1)-[w2s1:word2sid]->(sid1:sid)
,(w2)-[w2s2:word2sid]->(sid2:sid)
,(w3)-[w2s3:word2sid]->(sid3:sid)
WHERE (w2s1.pos STARTS WITH 'NN' AND w2s2.pos STARTS WITH 'VB' AND w2s3.pos STARTS WITH 'JJ')
OR (w2s1.pos STARTS WITH 'JJ' AND w2s2.pos STARTS WITH 'TO' AND w2s3.pos STARTS WITH 'VB')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'JJ' AND w2s3.pos STARTS WITH 'NN')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'IN' AND w2s3.pos STARTS WITH 'NN')
WITH p, COUNT(p) AS pcnt, COUNT(DISTINCT w2s1) AS w1cnt, COUNT(DISTINCT w2s2) AS w2cnt, COUNT(DISTINCT w2s3) AS w3cnt, LENGTH(p) AS plen
WITH p, pcnt, w1cnt, w2cnt, w3cnt, plen, 1.0/toFloat(1.0/(1.2*w1cnt)+1.0/w2cnt+1.0/(1.5*w3cnt))*toFloat(plen) AS score
WHERE score > 10
RETURN extract(n in nodes(p)|n.s) AS sent, score
LIMIT 100
  '''
  res = db.run(query)
  sentences = [x.values() for x in res.records()]
  # print sentences
  # sentences = [([u'Motel', u'One', u'is', u'really', u'good'], 11.851851851851851), ([u'room', u'which', u'is', u'really', u'good'], 12.272727272727272), ([u'room', u'which', u'was', u'to', u'new'], 11.423076923076925), ([u'right', u'in', u'this', u'location'], 10.318471337579618), ([u'Motel', u'One', u'is', u'also', u'small'], 10.176678445229681), ([u'room', u'which', u'is', u'also', u'small'], 10.485436893203884), ([u'right', u'in', u'the', u'room'], 10.318471337579618), ([u'very', u'nice', u'with', u'a', u'room'], 11.11764705882353), ([u'right', u'in', u'a', u'room'], 10.318471337579618)]
  res = []
  from collections import defaultdict
  suffix_augmented_sentences = defaultdict(set)
  prefix_augmented_sentences = defaultdict(set)
  for i in range(len(sentences)):
    for j in range(i+1, len(sentences)):
      s1, m1 = sentences[i]
      s1 = tuple(s1)
      s2, m2 = sentences[j]
      s2 = tuple(s2)
      cnt = 0
      while s1[-cnt] == s2[-cnt]:
        cnt += 1
      if cnt > 2:
        suffix_augmented_sentences[s1[-cnt:]].update([(s1[:-cnt], m1), (s2[:-cnt], m2)])
      cnt = 0
      while s1[cnt] == s2[cnt]:
        cnt += 1
      if cnt > 2:
        prefix_augmented_sentences[s1[:cnt]].update([(s1[cnt:], m1), (s2[cnt:], m2)])
  for prefix, others in prefix_augmented_sentences.items():
    collepsed = reduce(lambda y, x: (y[0]+', '+ ' '.join(x[0]), y[1]+0.8*x[1]), others, ('', 0))
    res.append((" ".join(prefix) + " " + collepsed[0][2:],  collepsed[1]))
  for suffix, others in suffix_augmented_sentences.items():
    collepsed = reduce(lambda y, x: (y[0]+', '+ ' '.join(x[0]), y[1]+0.8*x[1]), others, ('', 0))
    res.append((collepsed[0][2:] + " " + " ".join(prefix),  collepsed[1]))
  res += [(" ".join(x[0]), x[1]) for x in sentences]
  res = sorted(res, key=lambda x:x[1], reverse=True)
  for x in res:
    print(x)

  '''
(u'room which is really good, also small', 18.206531332744927)
(u'Motel One is also small, really good', 17.622824237665228)
(u'room which is really good', 12.272727272727272)
(u'Motel One is really good', 11.851851851851851)
(u'room which was to new', 11.423076923076925)
(u'very nice with a room', 11.11764705882353)
(u'room which is also small', 10.485436893203884)
(u'right in this location', 10.318471337579618)
(u'right in the room', 10.318471337579618)
(u'right in a room', 10.318471337579618)
(u'Motel One is also small', 10.176678445229681)
'''

if __name__ == '__main__':
  # create_graph()
  # create_summary()
  test_create_summary()
      

'''
MATCH (w1:word)-[:next]->(w2:word)-[:next]->(w3:word),
(w1)-[:word2sid]->(sid:sid)-[:sid2rid]->(rid:rid)
WHERE
(w2)-[:word2sid]->(sid)-[:sid2rid]->(rid) AND
(w3)-[:word2sid]->(sid)-[:sid2rid]->(rid)
RETURN w1.s, w2.s, w3.s, sid.v, rid.v
LIMIT 100

MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
WITH w1, w2, w3, COUNT(p) AS cnt
WHERE cnt > 1
RETURN w1.s, w2.s, w3.s, cnt
LIMIT 10

MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
,(w1)-[w2s1:word2sid]->(sid1:sid)
,(w2)-[w2s2:word2sid]->(sid2:sid)
,(w3)-[w2s3:word2sid]->(sid3:sid)
WHERE (w2s1.pos STARTS WITH 'NN' AND w2s2.pos STARTS WITH 'VB' AND w2s3.pos STARTS WITH 'JJ')
OR (w2s1.pos STARTS WITH 'JJ' AND w2s2.pos STARTS WITH 'TO' AND w2s3.pos STARTS WITH 'VB')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'JJ' AND w2s3.pos STARTS WITH 'NN')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'IN' AND w2s3.pos STARTS WITH 'NN')
WITH w1.s AS w1s, w2.s AS w2s, w3.s AS w3s, w2s1.pos AS w2s1pos, w2s2.pos AS w2s2pos, w2s3.pos AS w2s3pos, COUNT(p) AS cnt
WHERE cnt > 5
RETURN w1s, w2s, w3s, w2s1pos, w2s2pos, w2s3pos, cnt
LIMIT 10

╒════════╤═══════╤════════╤═════════╤═════════╤═════════╤═════╕
│"w1s"   │"w2s"  │"w3s"   │"w2s1pos"│"w2s2pos"│"w2s3pos"│"cnt"│
╞════════╪═══════╪════════╪═════════╪═════════╪═════════╪═════╡
│"street"│"are"  │"many"  │"NN"     │"VBP"    │"JJ"     │12   │
├────────┼───────┼────────┼─────────┼─────────┼─────────┼─────┤
│"bus"   │"have" │"open"  │"NN"     │"VBP"    │"JJ"     │9    │
├────────┼───────┼────────┼─────────┼─────────┼─────────┼─────┤
│"Das"   │"ist"  │"sollte"│"NNP"    │"VBP"    │"JJ"     │12   │
├────────┼───────┼────────┼─────────┼─────────┼─────────┼─────┤
│"nights"│"have" │"bed"   │"NNS"    │"VB"     │"JJ"     │8    │
├────────┼───────┼────────┼─────────┼─────────┼─────────┼─────┤
│"back"  │"with" │"longer"│"RB"     │"IN"     │"NN"     │6    │
├────────┼───────┼────────┼─────────┼─────────┼─────────┼─────┤
│"only"  │"in"   │"corner"│"RB"     │"IN"     │"NN"     │40   │
├────────┼───────┼────────┼─────────┼─────────┼─────────┼─────┤
│"town"  │"ist"  │"nur"   │"NN"     │"VBD"    │"JJ"     │10   │
├────────┼───────┼────────┼─────────┼─────────┼─────────┼─────┤
│"just"  │"in"   │"Castle"│"RB"     │"IN"     │"NNP"    │80   │
├────────┼───────┼────────┼─────────┼─────────┼─────────┼─────┤
│"there" │"are"  │"big"   │"NN"     │"VBP"    │"JJ"     │12   │
├────────┼───────┼────────┼─────────┼─────────┼─────────┼─────┤
│"eher"  │"hotel"│"hier"  │"NN"     │"VBD"    │"JJR"    │6    │
└────────┴───────┴────────┴─────────┴─────────┴─────────┴─────┘

MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
,(w1)-[w2s1:word2sid]->(sid1:sid)
,(w2)-[w2s2:word2sid]->(sid2:sid)
,(w3)-[w2s3:word2sid]->(sid3:sid)
WHERE (w2s1.pos STARTS WITH 'NN' AND w2s2.pos STARTS WITH 'VB' AND w2s3.pos STARTS WITH 'JJ')
OR (w2s1.pos STARTS WITH 'JJ' AND w2s2.pos STARTS WITH 'TO' AND w2s3.pos STARTS WITH 'VB')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'JJ' AND w2s3.pos STARTS WITH 'NN')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'IN' AND w2s3.pos STARTS WITH 'NN')
WITH p, COUNT(p) AS cnt
WHERE cnt > 5
RETURN extract(n in nodes(p)|n.s), cnt
LIMIT 10

╒═══════════════════════════════════════════╤═════╕
│"extract(n in nodes(p)|n.s)"               │"cnt"│
╞═══════════════════════════════════════════╪═════╡
│["right","in","a","longer"]                │100  │
├───────────────────────────────────────────┼─────┤
│["reasonably","priced","on","the","street"]│6    │
├───────────────────────────────────────────┼─────┤
│["Menschen",",","die","sich"]              │6    │
├───────────────────────────────────────────┼─────┤
│["shower","was","also","small"]            │66   │
├───────────────────────────────────────────┼─────┤
│["Breakfast","is","a","German"]            │15   │
├───────────────────────────────────────────┼─────┤
│["One","is","continental-style"]           │30   │
├───────────────────────────────────────────┼─────┤
│["ist","hier","ist"]                       │15   │
├───────────────────────────────────────────┼─────┤
│["back","and","comfortable",",","aber"]    │12   │
├───────────────────────────────────────────┼─────┤
│[")","and","comfortable",",","aber"]       │12   │
├───────────────────────────────────────────┼─────┤
│["only","one","of","the","front"]          │72   │
└───────────────────────────────────────────┴─────┘


MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
,(w1)-[w2s1:word2sid]->(sid1:sid)
,(w2)-[w2s2:word2sid]->(sid2:sid)
,(w3)-[w2s3:word2sid]->(sid3:sid)
WHERE (w2s1.pos STARTS WITH 'NN' AND w2s2.pos STARTS WITH 'VB' AND w2s3.pos STARTS WITH 'JJ')
OR (w2s1.pos STARTS WITH 'JJ' AND w2s2.pos STARTS WITH 'TO' AND w2s3.pos STARTS WITH 'VB')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'JJ' AND w2s3.pos STARTS WITH 'NN')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'IN' AND w2s3.pos STARTS WITH 'NN')
WITH p, COUNT(p) AS pcnt, COUNT(DISTINCT w2s1) AS w1cnt, COUNT(DISTINCT w2s2) AS w2cnt, COUNT(DISTINCT w2s3) AS w3cnt, LENGTH(p) AS plen
WITH p, pcnt, w1cnt, w2cnt, w3cnt, plen, toFloat(pcnt+w1cnt+w2cnt+w3cnt)/toFloat(plen) AS cnt
RETURN extract(n in nodes(p)|n.s), pcnt, w1cnt, w2cnt, w3cnt, plen, cnt
LIMIT 10

╒═══════════════════════════════════════════╤══════╤═══════╤═══════╤═══════╤══════╤══════════════════╕
│"extract(n in nodes(p)|n.s)"               │"pcnt"│"w1cnt"│"w2cnt"│"w3cnt"│"plen"│"cnt"             │
╞═══════════════════════════════════════════╪══════╪═══════╪═══════╪═══════╪══════╪══════════════════╡
│["Airlink-Flughafenbus","kann","man","auf"]│3     │1      │3      │1      │3     │2.6666666666666665│
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["One","is","inviting","and","my"]         │2     │2      │1      │1      │4     │1.5               │
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["room","is","inviting","and","my"]        │9     │9      │1      │1      │4     │5                 │
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["Breakfast","is","inviting","and","my"]   │1     │1      │1      │1      │4     │1                 │
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["there","is","inviting","and","my"]       │1     │1      │1      │1      │4     │1                 │
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["reception","is","inviting","and","my"]   │1     │1      │1      │1      │4     │1                 │
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["Location","is","inviting","and","my"]    │4     │4      │1      │1      │4     │2.5               │
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["Mile","is","inviting","and","my"]        │4     │4      │1      │1      │4     │2.5               │
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["only","one","in","every","day"]          │40    │2      │20     │1      │4     │15.75             │
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["still","one","in","every","day"]         │40    │2      │20     │1      │4     │15.75             │
└───────────────────────────────────────────┴──────┴───────┴───────┴───────┴──────┴──────────────────┘


MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
,(w1)-[w2s1:word2sid]->(sid1:sid)
,(w2)-[w2s2:word2sid]->(sid2:sid)
,(w3)-[w2s3:word2sid]->(sid3:sid)
WHERE (w2s1.pos STARTS WITH 'NN' AND w2s2.pos STARTS WITH 'VB' AND w2s3.pos STARTS WITH 'JJ')
OR (w2s1.pos STARTS WITH 'JJ' AND w2s2.pos STARTS WITH 'TO' AND w2s3.pos STARTS WITH 'VB')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'JJ' AND w2s3.pos STARTS WITH 'NN')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'IN' AND w2s3.pos STARTS WITH 'NN')
WITH p, COUNT(p) AS pcnt, COUNT(DISTINCT w2s1) AS w1cnt, COUNT(DISTINCT w2s2) AS w2cnt, COUNT(DISTINCT w2s3) AS w3cnt, LENGTH(p) AS plen LIMIT 10
WITH p, pcnt, w1cnt, w2cnt, w3cnt, plen, 1.0/toFloat(1.0/w1cnt+1.0/w2cnt+1.0/w3cnt)*toFloat(plen) AS cnt
RETURN extract(n in nodes(p)|n.s), pcnt, w1cnt, w2cnt, w3cnt, plen, cnt
LIMIT 10

╒═══════════════════════════════════════════╤══════╤═══════╤═══════╤═══════╤══════╤══════════════════╕
│"extract(n in nodes(p)|n.s)"               │"pcnt"│"w1cnt"│"w2cnt"│"w3cnt"│"plen"│"cnt"             │
╞═══════════════════════════════════════════╪══════╪═══════╪═══════╪═══════╪══════╪══════════════════╡
│["Airlink-Flughafenbus","kann","man","auf"]│3     │1      │3      │1      │3     │1.2857142857142858│
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["One","is","inviting","and","my"]         │2     │2      │1      │1      │4     │1.6               │
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["room","is","inviting","and","my"]        │9     │9      │1      │1      │4     │1.894736842105263 │
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["Breakfast","is","inviting","and","my"]   │1     │1      │1      │1      │4     │1.3333333333333333│
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["there","is","inviting","and","my"]       │1     │1      │1      │1      │4     │1.3333333333333333│
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["reception","is","inviting","and","my"]   │1     │1      │1      │1      │4     │1.3333333333333333│
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["Location","is","inviting","and","my"]    │4     │4      │1      │1      │4     │1.7777777777777777│
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["Mile","is","inviting","and","my"]        │4     │4      │1      │1      │4     │1.7777777777777777│
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["only","one","in","every","day"]          │40    │2      │20     │1      │4     │2.5806451612903225│
├───────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["still","one","in","every","day"]         │40    │2      │20     │1      │4     │2.5806451612903225│
└───────────────────────────────────────────┴──────┴───────┴───────┴───────┴──────┴──────────────────┘

MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
,(w1)-[w2s1:word2sid]->(sid1:sid)
,(w2)-[w2s2:word2sid]->(sid2:sid)
,(w3)-[w2s3:word2sid]->(sid3:sid)
WHERE (w2s1.pos STARTS WITH 'NN' AND w2s2.pos STARTS WITH 'VB' AND w2s3.pos STARTS WITH 'JJ')
OR (w2s1.pos STARTS WITH 'JJ' AND w2s2.pos STARTS WITH 'TO' AND w2s3.pos STARTS WITH 'VB')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'JJ' AND w2s3.pos STARTS WITH 'NN')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'IN' AND w2s3.pos STARTS WITH 'NN')
WITH p, COUNT(p) AS pcnt, COUNT(DISTINCT w2s1) AS w1cnt, COUNT(DISTINCT w2s2) AS w2cnt, COUNT(DISTINCT w2s3) AS w3cnt, LENGTH(p) AS plen
WITH p, pcnt, w1cnt, w2cnt, w3cnt, plen, 1.0/toFloat(1.0/w1cnt+1.0/w2cnt+1.0/w3cnt)*toFloat(plen) AS score
WHERE score > 5
RETURN extract(n in nodes(p)|n.s), pcnt, w1cnt, w2cnt, w3cnt, plen, score
LIMIT 10

╒════════════════════════════════════════╤══════╤═══════╤═══════╤═══════╤══════╤═════════════════╕
│"extract(n in nodes(p)|n.s)"            │"pcnt"│"w1cnt"│"w2cnt"│"w3cnt"│"plen"│"score"          │
╞════════════════════════════════════════╪══════╪═══════╪═══════╪═══════╪══════╪═════════════════╡
│["still","one","for","a","room"]        │198   │2      │11     │9      │4     │5.697841726618705│
├────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼─────────────────┤
│["only","one","for","a","room"]         │198   │2      │11     │9      │4     │5.697841726618705│
├────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼─────────────────┤
│["room","is","also","small"]            │405   │9      │15     │3      │3     │5.869565217391305│
├────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼─────────────────┤
│["room",",","ist",",","comfortable"]    │81    │9      │3      │3      │4     │5.142857142857143│
├────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼─────────────────┤
│["location",",","ist",",","comfortable"]│81    │9      │3      │3      │4     │5.142857142857143│
├────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼─────────────────┤
│["just","handled","in","the","room"]    │360   │2      │20     │9      │4     │6.050420168067226│
├────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼─────────────────┤
│["still","one","in","the","room"]       │360   │2      │20     │9      │4     │6.050420168067226│
├────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼─────────────────┤
│["only","one","in","the","room"]        │360   │2      │20     │9      │4     │6.050420168067226│
├────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼─────────────────┤
│["Motel","One","is","really","good"]    │480   │8      │15     │4      │4     │9.056603773584905│
├────────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼─────────────────┤
│["room","which","is","really","good"]   │540   │9      │15     │4      │4     │9.350649350649352│
└────────────────────────────────────────┴──────┴───────┴───────┴───────┴──────┴─────────────────┘

MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
,(w1)-[w2s1:word2sid]->(sid1:sid)
,(w2)-[w2s2:word2sid]->(sid2:sid)
,(w3)-[w2s3:word2sid]->(sid3:sid)
WHERE (w2s1.pos STARTS WITH 'NN' AND w2s2.pos STARTS WITH 'VB' AND w2s3.pos STARTS WITH 'JJ')
OR (w2s1.pos STARTS WITH 'JJ' AND w2s2.pos STARTS WITH 'TO' AND w2s3.pos STARTS WITH 'VB')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'JJ' AND w2s3.pos STARTS WITH 'NN')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'IN' AND w2s3.pos STARTS WITH 'NN')
WITH p, COUNT(p) AS pcnt, COUNT(DISTINCT w2s1) AS w1cnt, COUNT(DISTINCT w2s2) AS w2cnt, COUNT(DISTINCT w2s3) AS w3cnt, LENGTH(p) AS plen
WITH p, pcnt, w1cnt, w2cnt, w3cnt, plen, 1.0/toFloat(1.0/w1cnt+1.0/w2cnt+1.0/w3cnt)*toFloat(plen) AS score
WHERE score > 10
RETURN extract(n in nodes(p)|n.s), pcnt, w1cnt, w2cnt, w3cnt, plen, score
LIMIT 10

(no changes, no records)

MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
,(w1)-[w2s1:word2sid]->(sid1:sid)
,(w2)-[w2s2:word2sid]->(sid2:sid)
,(w3)-[w2s3:word2sid]->(sid3:sid)
WHERE (w2s1.pos STARTS WITH 'NN' AND w2s2.pos STARTS WITH 'VB' AND w2s3.pos STARTS WITH 'JJ')
OR (w2s1.pos STARTS WITH 'JJ' AND w2s2.pos STARTS WITH 'TO' AND w2s3.pos STARTS WITH 'VB')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'JJ' AND w2s3.pos STARTS WITH 'NN')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'IN' AND w2s3.pos STARTS WITH 'NN')
WITH p, COUNT(p) AS pcnt, COUNT(DISTINCT w2s1) AS w1cnt, COUNT(DISTINCT w2s2) AS w2cnt, COUNT(DISTINCT w2s3) AS w3cnt, LENGTH(p) AS plen
WITH p, pcnt, w1cnt, w2cnt, w3cnt, plen, 1.0/toFloat(1.0/(2*w1cnt)+1.0/w2cnt+1.0/(3*w3cnt))*toFloat(plen) AS score
WHERE score > 5
RETURN extract(n in nodes(p)|n.s), pcnt, w1cnt, w2cnt, w3cnt, plen, score

╒══════════════════════════════════════╤══════╤═══════╤═══════╤═══════╤══════╤══════════════════╕
│"extract(n in nodes(p)|n.s)"          │"pcnt"│"w1cnt"│"w2cnt"│"w3cnt"│"plen"│"score"           │
╞══════════════════════════════════════╪══════╪═══════╪═══════╪═══════╪══════╪══════════════════╡
│["only","one","in","every","day"]     │40    │2      │20     │1      │4     │6.315789473684211 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["still","one","in","every","day"]    │40    │2      │20     │1      │4     │6.315789473684211 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["just","handled","in","every","day"] │40    │2      │20     │1      │4     │6.315789473684211 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["so","noch","in","every","day"]      │60    │3      │20     │1      │4     │7.2727272727272725│
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["ist","hier","ist",",","sollte"]     │30    │5      │3      │2      │4     │6.666666666666667 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["man","hier","ist",",","sollte"]     │42    │7      │3      │2      │4     │7                 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["only","one","of","the","budget"]    │24    │2      │12     │1      │4     │6                 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["still","one","of","the","budget"]   │24    │2      │12     │1      │4     │6                 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["so","the","bed",",","Edinburgh"]    │54    │3      │2      │9      │4     │5.684210526315789 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["only","one","for","the","5th"]      │22    │2      │11     │1      │4     │5.932584269662921 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["still","one","for","the","5th"]     │22    │2      │11     │1      │4     │5.932584269662921 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["side","the","bed",",","comfortable"]│18    │3      │2      │3      │4     │5.142857142857143 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["location",",","ist","hier"]         │81    │9      │3      │3      │3     │6.000000000000002 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["just","handled","in","a","bit"]     │80    │2      │20     │2      │4     │8.571428571428571 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["still","one","in","a","bit"]        │80    │2      │20     │2      │4     │8.571428571428571 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["only","one","in","a","bit"]         │80    │2      │20     │2      │4     │8.571428571428571 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["so","noch","in","a","bit"]          │120   │3      │20     │2      │4     │10.434782608695652│
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["Das","Personal","ist",",","good"]   │36    │3      │3      │4      │4     │6.857142857142857 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["very","nice","with","all","within"] │42    │7      │6      │1      │4     │7                 │
├──────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["room","is","a","European"]          │135   │9      │15     │1      │3     │6.585365853658537 │
└──────────────────────────────────────┴──────┴───────┴───────┴───────┴──────┴──────────────────┘


MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
,(w1)-[w2s1:word2sid]->(sid1:sid)
,(w2)-[w2s2:word2sid]->(sid2:sid)
,(w3)-[w2s3:word2sid]->(sid3:sid)
WHERE (w2s1.pos STARTS WITH 'NN' AND w2s2.pos STARTS WITH 'VB' AND w2s3.pos STARTS WITH 'JJ')
OR (w2s1.pos STARTS WITH 'JJ' AND w2s2.pos STARTS WITH 'TO' AND w2s3.pos STARTS WITH 'VB')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'JJ' AND w2s3.pos STARTS WITH 'NN')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'IN' AND w2s3.pos STARTS WITH 'NN')
WITH p, COUNT(p) AS pcnt, COUNT(DISTINCT w2s1) AS w1cnt, COUNT(DISTINCT w2s2) AS w2cnt, COUNT(DISTINCT w2s3) AS w3cnt, LENGTH(p) AS plen
WITH p, pcnt, w1cnt, w2cnt, w3cnt, plen, 1.0/toFloat(1.0/(2*w1cnt)+1.0/w2cnt+1.0/(3*w3cnt))*toFloat(plen) AS score
WHERE score > 10
RETURN extract(n in nodes(p)|n.s), pcnt, w1cnt, w2cnt, w3cnt, plen, score
LIMIT 10

╒═════════════════════════════════════╤══════╤═══════╤═══════╤═══════╤══════╤══════════════════╕
│"extract(n in nodes(p)|n.s)"         │"pcnt"│"w1cnt"│"w2cnt"│"w3cnt"│"plen"│"score"           │
╞═════════════════════════════════════╪══════╪═══════╪═══════╪═══════╪══════╪══════════════════╡
│["so","noch","in","a","bit"]         │120   │3      │20     │2      │4     │10.434782608695652│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["room","is","a","bed"]              │270   │9      │15     │2      │3     │10.384615384615385│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["still","one","for","a","room"]     │198   │2      │11     │9      │4     │10.583518930957682│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["only","one","for","a","room"]      │198   │2      │11     │9      │4     │10.583518930957682│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["room","is","also","small"]         │405   │9      │15     │3      │3     │12.857142857142858│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["just","handled","in","the","room"] │360   │2      │20     │9      │4     │11.868131868131869│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["still","one","in","the","room"]    │360   │2      │20     │9      │4     │11.868131868131869│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["only","one","in","the","room"]     │360   │2      │20     │9      │4     │11.868131868131869│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["Motel","One","is","really","good"] │480   │8      │15     │4      │4     │18.82352941176471 │
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["room","which","is","really","good"]│540   │9      │15     │4      │4     │19.45945945945946 │
└─────────────────────────────────────┴──────┴───────┴───────┴───────┴──────┴──────────────────┘

MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
,(w1)-[w2s1:word2sid]->(sid1:sid)
,(w2)-[w2s2:word2sid]->(sid2:sid)
,(w3)-[w2s3:word2sid]->(sid3:sid)
WHERE (w2s1.pos STARTS WITH 'NN' AND w2s2.pos STARTS WITH 'VB' AND w2s3.pos STARTS WITH 'JJ')
OR (w2s1.pos STARTS WITH 'JJ' AND w2s2.pos STARTS WITH 'TO' AND w2s3.pos STARTS WITH 'VB')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'JJ' AND w2s3.pos STARTS WITH 'NN')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'IN' AND w2s3.pos STARTS WITH 'NN')
WITH p, COUNT(p) AS pcnt, COUNT(DISTINCT w2s1) AS w1cnt, COUNT(DISTINCT w2s2) AS w2cnt, COUNT(DISTINCT w2s3) AS w3cnt, LENGTH(p) AS plen
WITH p, pcnt, w1cnt, w2cnt, w3cnt, plen, 1.0/toFloat(1.0/(1.2*w1cnt)+1.0/w2cnt+1.0/(1.5*w3cnt))*toFloat(plen) AS score
WHERE score > 10
RETURN extract(n in nodes(p)|n.s), pcnt, w1cnt, w2cnt, w3cnt, plen, score
LIMIT 10

╒═════════════════════════════════════╤══════╤═══════╤═══════╤═══════╤══════╤══════════════════╕
│"extract(n in nodes(p)|n.s)"         │"pcnt"│"w1cnt"│"w2cnt"│"w3cnt"│"plen"│"score"           │
╞═════════════════════════════════════╪══════╪═══════╪═══════╪═══════╪══════╪══════════════════╡
│["Motel","One","is","really","good"] │480   │8      │15     │4      │4     │11.851851851851851│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["room","which","is","really","good"]│540   │9      │15     │4      │4     │12.272727272727272│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["room","which","was","to","new"]    │396   │9      │11     │4      │4     │11.423076923076925│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["right","in","this","location"]     │900   │5      │20     │9      │3     │10.318471337579618│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["Motel","One","is","also","small"]  │360   │8      │15     │3      │4     │10.176678445229681│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["room","which","is","also","small"] │405   │9      │15     │3      │4     │10.485436893203884│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["right","in","the","room"]          │900   │5      │20     │9      │3     │10.318471337579618│
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["very","nice","with","a","room"]    │378   │7      │6      │9      │4     │11.11764705882353 │
├─────────────────────────────────────┼──────┼───────┼───────┼───────┼──────┼──────────────────┤
│["right","in","a","room"]            │900   │5      │20     │9      │3     │10.318471337579618│
└─────────────────────────────────────┴──────┴───────┴───────┴───────┴──────┴──────────────────┘
Started streaming 9 records after 76083 ms and completed after 76198 ms.


MATCH (n:sid)
WHERE (n)-[*2]->(:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})
WITH COUNT(n) AS scnt
MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
WHERE (:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})<-[*2]-()<-[:word2sid]-(w1)
AND (:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})<-[*2]-()<-[:word2sid]-(w2)
AND (:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})<-[*2]-()<-[:word2sid]-(w3)
WITH DISTINCT p, w1, w2, w3, scnt LIMIT 1000
MATCH (w1)-[w2s1:word2sid]->(sid1:sid)
,(w2)-[w2s2:word2sid]->(sid2:sid)
,(w3)-[w2s3:word2sid]->(sid3:sid)
WHERE 
(sid1)-[*2]->(:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})
AND (sid2)-[*2]->(:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})
AND (sid3)-[*2]->(:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})
AND ((w2s1.pos STARTS WITH 'NN' AND w2s2.pos STARTS WITH 'VB' AND w2s3.pos STARTS WITH 'JJ')
OR (w2s1.pos STARTS WITH 'JJ' AND w2s2.pos STARTS WITH 'TO' AND w2s3.pos STARTS WITH 'VB')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'JJ' AND w2s3.pos STARTS WITH 'NN')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'IN' AND w2s3.pos STARTS WITH 'NN'))
WITH scnt, p, COUNT(p) AS pcnt, COUNT(DISTINCT w2s1) AS w1cnt, COUNT(DISTINCT w2s2) AS w2cnt, COUNT(DISTINCT w2s3) AS w3cnt, LENGTH(p) AS plen
WITH scnt, p, pcnt, w1cnt, w2cnt, w3cnt, plen, 1.0/toFloat(1.0/(1.2*w1cnt)+1.0/w2cnt+1.0/(1.5*w3cnt))*toFloat(plen) AS score
WHERE score > scnt/10
RETURN extract(n in nodes(p)|n.s), pcnt, w1cnt, w2cnt, w3cnt, plen, score, scnt
LIMIT 10



MATCH (n:rid)
WHERE (n)-[*1]->(:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})
WITH n LIMIT 1000
RETURN COUNT(n)
╒══════════╕
│"COUNT(n)"│
╞══════════╡
│10        │
└──────────┘

MATCH (n:sid)
WHERE (n)-[*2]->(:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})
WITH n LIMIT 1000
RETURN COUNT(n)
╒══════════╕
│"COUNT(n)"│
╞══════════╡
│80        │
└──────────┘

MATCH (n:word)
WHERE (n)-[:word2sid]->()-[*2]->(:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})
WITH n LIMIT 1000
RETURN COUNT(n)
╒══════════╕
│"COUNT(n)"│
╞══════════╡
│636       │
└──────────┘

MATCH (n:word)
WHERE (:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})<-[*2]-()<-[:word2sid]-(n)
RETURN COUNT(n)
╒══════════╕
│"COUNT(n)"│
╞══════════╡
│1002      │
└──────────┘

MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
WHERE (:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})<-[*2]-()<-[:word2sid]-(w1)
AND (:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})<-[*2]-()<-[:word2sid]-(w2)
AND (:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})<-[*2]-()<-[:word2sid]-(w3)
RETURN DISTINCT w1,w2,w3
LIMIT 1000

Started streaming 1000 records after 47 ms and completed after 17133 ms.
╒═══════════════════╤════════════════╤═════════════╕
│"w1"               │"w2"            │"w3"         │
╞═══════════════════╪════════════════╪═════════════╡
│{"s":"of"}         │{"s":"attitude"}│{"s":"about"}│
├───────────────────┼────────────────┼─────────────┤
│{"s":"tons"}       │{"s":"attitude"}│{"s":"about"}│
├───────────────────┼────────────────┼─────────────┤
│{"s":"side"}       │{"s":"attitude"}│{"s":"about"}│
├───────────────────┼────────────────┼─────────────┤
│{"s":"day"}        │{"s":"attitude"}│{"s":"about"}│
├───────────────────┼────────────────┼─────────────┤
│{"s":"All"}        │{"s":"attitude"}│{"s":"about"}│
├───────────────────┼────────────────┼─────────────┤
│{"s":"center"}     │{"s":"attitude"}│{"s":"about"}│
├───────────────────┼────────────────┼─────────────┤
│{"s":"idea"}       │{"s":"attitude"}│{"s":"about"}│
├───────────────────┼────────────────┼─────────────┤
│{"s":"town"}       │{"s":"attitude"}│{"s":"about"}│
├───────────────────┼────────────────┼─────────────┤
│{"s":"middle"}     │{"s":"attitude"}│{"s":"about"}│
├───────────────────┼────────────────┼─────────────┤
│{"s":"group"}      │{"s":"attitude"}│{"s":"about"}│
├───────────────────┼────────────────┼─────────────┤

MATCH p=(w1:word)-[:next*1..2]->(w2:word)-[:next*1..2]->(w3:word)
WHERE (:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})<-[*2]-()<-[:word2sid]-(w1)
AND (:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})<-[*2]-()<-[:word2sid]-(w2)
AND (:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})<-[*2]-()<-[:word2sid]-(w3)
WITH DISTINCT p, w1, w2, w3 LIMIT 100
MATCH (w1)-[w2s1:word2sid]->(sid1:sid)
MATCH (w2)-[w2s2:word2sid]->(sid2:sid)
MATCH (w3)-[w2s3:word2sid]->(sid3:sid)
WHERE 
(sid1)-[*2]->(:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})
AND (sid2)-[*2]->(:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})
AND (sid3)-[*2]->(:bid {v:'uYHaNptLzDLoV_JZ_MuzUA'})
AND ((w2s1.pos STARTS WITH 'NN' AND w2s2.pos STARTS WITH 'VB' AND w2s3.pos STARTS WITH 'JJ')
OR (w2s1.pos STARTS WITH 'JJ' AND w2s2.pos STARTS WITH 'TO' AND w2s3.pos STARTS WITH 'VB')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'JJ' AND w2s3.pos STARTS WITH 'NN')
OR (w2s1.pos STARTS WITH 'RB' AND w2s2.pos STARTS WITH 'IN' AND w2s3.pos STARTS WITH 'NN'))
WITH p, COUNT(p) AS pcnt, COUNT(DISTINCT w2s1) AS w1cnt, COUNT(DISTINCT w2s2) AS w2cnt, COUNT(DISTINCT w2s3) AS w3cnt, LENGTH(p) AS plen
WITH p, pcnt, w1cnt, w2cnt, w3cnt, plen, 1.0/toFloat(1.0/(1.2*w1cnt)+1.0/w2cnt+1.0/(1.5*w3cnt))*toFloat(plen) AS score
WHERE score > 10
RETURN DISTINCT reduce(x='', cha in extract(n in nodes(p)|n.s) | x+cha+' ') AS sent, score

'''