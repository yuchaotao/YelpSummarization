import json

# The order of review may be random
# TO DO: sort the review or use Spark to solve this

if __name__ == '__main__':
    tagged_review = open('../data/tagged_review.json','r')
    cnt = 0
    business_id = ''
    file_cnt = 0
    while True:
        line = tagged_review.readline()
        if not line:
            break
        line = json.loads(line)
        if line['business_id'] == business_id:
            res += line['text']
            cnt += len(line['text'])
        else:
            if cnt >= 60:
                f = open('input/%s'%business_id, 'w')
                for sent in res:
                    print >>f, sent.encode('utf8')
                file_cnt += 1
                if file_cnt == 200:
                    break
            else:
                pass
            business_id = line['business_id']
            res = line['text']
            cnt = len(res)
