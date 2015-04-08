import datetime
import json
import re
import uuid
import logging
import time

import pymongo
from pymongo import Connection

def safe_modify(col, query, update, upsert=False):
    for attempt in range(5):
        try:
            result = col.find_and_modify(
                    query=query,
                    update=update,
                    upsert=upsert,
                    new=True
            )
            return result
        except pymongo.errors.OperationFailure:
            return False
        except pymongo.errors.AutoReconnect:
            wait_t = 0.5 * pow(2, attempt)
            time.sleep(wait_t)
    return False


def safe_insert(collection, data):
    for attempt in range(5):
        try:
            collection.insert(data, safe=True)
            return True
        except pymongo.errors.OperationFailure:
            return False
        except pymongo.errors.AutoReconnect:
            wait_t = 0.5 * pow(2, attempt)
            time.sleep(wait_t)
    return False

def find_by_email(emails):
    return dict([(x['details']['email'], x['_id'].hex) for x in \
            Connection().ppau.members.find({
                "details.email": { "$in": emails }})])

def find_by_name_pair(namepairs):
    out = []
    coll = Connection().ppau.members
    for (fn, sn, email) in namepairs:
        record = coll.find_one({"details.given_names": re.compile(fn, re.I),
                                "details.surname": re.compile(sn, re.I)})
        if record is not None:
            out.append((email, record['_id'].hex))
    return dict(out)

def resign_them_all(data):
    emails = [x[2] for x in data]

    mm = find_by_email(emails)

    namepairs = [(x[0], x[1], x[2]) for x in data if x[2] not in mm]

    mm.update(find_by_name_pair(namepairs))

    fail = [x for x in data if x[2] not in mm]

    print (fail)

