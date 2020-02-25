from pymongo import MongoClient
def validate():
    client = MongoClient()
    db = client['salarydotcom']
    consul1_coll = db['consul3base']
    pipeline = [
    {'$group': {
        '_id': "$Job Role",
        #'uniqueIds': {'$addToSet': '$_id'},
        'count': {'$sum': 1}
        }
    }]
    #{'$match': {'count': {'$gt': 1}}}]
    res = list(consul1_coll.aggregate(pipeline))
    print(f'Length : {len(list(res))}')
    print(list(res))
    if not list(res):
        print('no duplicates!')
    for i in res:
        print(i)

if __name__=='__main__':
    validate()
