
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

class Neo4jHelper(object):

  def __init__(self):
    from neo4j.v1 import GraphDatabase, basic_auth
    self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "neo4j"))

  def run(self, *s):
    session = self.driver.session()
    res = session.run(*s)
    session.close()    

def test():
  db = Neo4jHelper()
  print db.run("CREATE (a:Person {name: {name}, title: {title}})",
               {"name": "Arthur", "title": "King"})
    

if __name__ == '__main__':
  db = Neo4jHelper()
  # db.run("MATCH (n) DETACH DELETE n")
  tagged_review_file = open("../data/tagged_review.json", "r")
  for line in tagged_review_file:
    data = json.loads(line)
    sid = data['review_id']
    rid = data['business_id']
    for sentence in data['text']:
      new_words = []
      pid = 0
      for tagged_word in sentence.split(' '):
        slash = tagged_word.rfind("/")
        word = tagged_word[:slash]
        pos = tagged_word[slash+1:]
        new_words.append((word, pos, pid))
        pid += 1
      word_syntax = '(:word {{s:"{word}"}})'
      rid_syntax = '(:rid {{v:"{rid}"}})'
      relation_word2rid_syntax = '-[:word2rid {{pos:"{pos}", pid:"{pid}", sid:"{sid}"}}]->'
      relation_word2word_syntax = '-[:next]->'
      # create or update words
      new_words_word = [new_word[0] for new_word in new_words]
      query = '\n'.join('MERGE (w{word}:word {{s:"{word}"}})'.format(word=new_word[0]) for new_word in new_words)
      query += '\nMERGE '+'-[:next]->'.join(map(lambda new_word: '(w{word})'.format(word=new_word[0]), new_words))
      print query
      exit(0)
      db.run(query)
      # create relations
      # 
      query = 'CREATE' + '-[:next]->'.join('(:word {{s:"{word}", pos:"{pos}", pid:{pid}, sid:"{sid}", rid:"{rid}"}})'\
      .format(word=x[0], pos=x[1], pid=x[2], sid=sid, rid=rid) for x in new_words)
      res = db.run(query)