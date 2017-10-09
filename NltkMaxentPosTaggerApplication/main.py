import mxpost
import cPickle as pickle
import json
from nltk.tokenize import word_tokenize
from nltk.tokenize import sent_tokenize
import time

class MaxentPosTagger(object):
    def __init__(self):
        try:
            self.tagger = pickle.load(open('MaxentPosTagger.pickle', 'r'))
        except:
            from nltk.corpus import treebank
            tagged_sents = treebank.tagged_sents()
            size = int(len(tagged_sents) * 0.1)
            train_sents, test_sents = tagged_sents[size:], tagged_sents[:size]
            self.tagger = mxpost.MaxentPosTagger()
            self.tagger.train(train_sents)
            print "tagger accuracy (test %i sentences, after training %i):" % \
                (size, size*9), self.tagger.evaluate(test_sents)
            pickle.dump(self.tagger, open('MaxentPosTagger.pickle', 'w'), -1)
        finally:
            print 'MaxentPosTagger Loadded'
    
    def __getattr__(self, attr):
        return self.tagger.__getattribute__(attr)

if __name__ == '__main__':
    time_start = time.time()
    maxent_tagger = MaxentPosTagger()
    review_file = open('../data/review.json', 'r')
    tagged_review_file = open('../data/tagged_review.json', 'w')
    total_lines = 4736897
    print 'Start Tagging'
    cnt = 0
    while True:
        line = review_file.readline()
        if not line:
            break
        record = json.loads(line)
        tagged_sentences = []
        for sentence in sent_tokenize(record['text']):
            words = word_tokenize(sentence)
            tagged_words = maxent_tagger.tag(words)
            tagged_sentence = ' '.join(map(lambda x: '/'.join(x), tagged_words))
            tagged_sentences.append(tagged_sentence)
        record['text'] = tagged_sentences
        print >> tagged_review_file, json.dumps(record)
        cnt += 1
        if cnt % 100 == 0:
            print 'transfering #%i/%i line (%%%f) of review.json | elapsed: %f min'%(cnt, total_lines, cnt*100.0/total_lines, (time.time()-time_start)/60)
