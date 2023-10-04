import unicodedata
from enum import Enum
from pymongo import MongoClient
from decouple import config


MONGO = config('MONGO_CONNECTION')
class Status(Enum):
    COMPLETE = "Complete"
    RUNNING = "Running"
    PENDING = "Pending"
    ERROR = "Error"

def normalize_field_value(text):
    text = (
        unicodedata.normalize('NFD', text)
        .encode('ascii', 'ignore')
        .decode('utf-8')
    )
    return str(text).upper()


def get_year(s, pre, suf):
    return int(s.replace(pre, '').replace(suf, ''))



def set_status(_doc:dict, status:Status, database:str)->None:
    _doc['status'] = status.value
    with MongoClient() as client:
        db = client["polygonize"]
        collection = db[database]
        collection.update_one({'_id':_doc['_id']},{'$set':_doc})

def get_complete(_doc:dict, database:str)->bool:
    with MongoClient() as client:
        db = client["polygonize"]
        collection = db[database]
        r = collection.find_one({
            '_id': _doc['_id'],
            'status': Status.COMPLETE
        })
        if r:
            return True
        return False