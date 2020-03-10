from pymongo import MongoClient
try:
    import secretmanager
except:
    import deployment_tmp.secret_manager as secretmanager


cluster = MongoClient(secretmanager.__MONGOURL__)

db = cluster["opensense"]
collection = db["vals"]

post_value_count = {
    "_id": 2,
    "valueCount": 0,
    "aimedValueCount": 0
}

post_handlecontentdataaction_count = {
    "_id": 5,
    "actionCount": 0
}


def main(args):
    id_to_print = args.get("printID", "")
    update = args.get("rewrite", "no")
    clear = args.get("clear", "no")
    if clear == "yes":
        # have to be run one time to init db
        collection.delete_many({})
    if update == "yes":
        # have to be run one time to init db
        collection.delete_many({})
        print(post_value_count)
        collection.insert_one(post_value_count)

        print(post_handlecontentdataaction_count)
        collection.insert_one(post_handlecontentdataaction_count)
    resultlist = []
    try:
        printID = int(id_to_print)
        if printID == 10:
            result = collection.find_one({"_id": 2})  # to find only one do find_one
            resultlist.append(result)
            result = collection.find_one({"_id": 5})  # to find only one do find_one
            resultlist.append(result)
        else:
            result = collection.find_one({"_id": printID})  # to find only one do find_one
            resultlist.append(result)
    except Exception as e:
        results = collection.find({})  # to find only one do find_one
        for result in results:
            resultlist.append(result)
        return {"message": "Document count {}".format(len(resultlist))}
    return {"message": resultlist}
