from sys import argv
import argparse
import time
import datetime
import riak
try:
	import json
except ImportError:
	import simplejson as json
from pymongo import MongoClient
import threading

parser = argparse.ArgumentParser()
parser.add_argument("-A", "--All", action='store_true', help="backup the entire db")
parser.add_argument("-a", "--account", type=int, help="backup a particular account")
parser.add_argument("-b", "--bucketname", help="backup a particular bucket")
parser.add_argument("-p", "--print_keys", help="prints all of the keys for a bucket")
parser.add_argument("-t", "--time", type=int, help="restricts backup to certain range")
parser.add_argument("-r", "--restore", help="changes setting to restore to a node instead")
args = parser.parse_args()
myClient = riak.RiakClient()
date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
typeList = ['TimelineEvents', 'TopoVersions', 'RiakClient', 'AgentCache']


# used with the -All option
# returns list of all buckets in the system using accountlist on mongodb
def getAccountsBuckets():
	# access mongodb to get list of accounts
	db = MongoClient().configdb_dev
	accountList = []
	for d in db.Accounts.find({},{"_id":True}):
		accountList.append(d["_id"])

	bucketList = []
	env = "dev"
	#TODO: query environment to get environment as env
	for account in accountList:
		for types in typeList:
			bucketList.append(env + ".ps." + types + "." + str(account))
	#return bucketList
	return ["dev.ps.TopoVersions.541a2ac73730e63fe202edcb", 'unittest.ps.TimelineEvents.539b47353004433b6f35a699', 'unittest.ps.ResourceVersions.5403787a3c376271d86418ae', 'unittest.ps.ResourceVersionSnapshots.539b47353004433b6f35a699', 'dev.ps.TopoVersions.52df169fdc797880d43ebfa9', 'dev.ps.TopoVersions.53fb72af6fa37bb52fcc9463', 'unittest.ps.ResourceVersions.539b47353004433b6f35a699']

# writes data and indices from bucket into target file
def writeBucket(bucket, target):
	keys = getKeys(bucket)
	for key in keys: 
	#for key in myClient.get_keys(bucket): #SLOW
		target.write(json.dumps(bucket.get(key).data))
		target.write('{ "indexes": [')
		for idx,val in bucket.get(key).indexes:
			target.write(json.dumps({idx:val}))
		target.write("]}\n")


# depending on time option is set, either returns all keys or just keys that fit a time query
def getKeys(bucket):
	if args.time:
		return bucket.get_index('timestamp_int', args.time, 99999999999999)
	else:
		start = time.time()
		#myClient.get_keys(bucket)
		bucket.get_index('$key','0','Z')
		end = time.time()
		print "Getting index time is " + str(end - start)	
		return bucket.get_index('$key','0','Z')	#return result could be HUGE. stream it with multiple loops perhaps. doesn't work with my bucket test

# used with the -printkeys option
# prints all of the keys in a bucket (for debugging purposes)
def printKeys(bucket):
	for keys in bucket.get_index('$key','0','Z'):
		print keys

	# for key in myClient.get_keys(bucket):
	# 	print key

	# for keys in bucket.stream_index('$key',0,'zzz', return_terms=True):
	# 	print keys	

# -b protocol to backup from a single bucket into a new file
def bucketProtocol():
	print "bucket protocol"
	bucketname = args.bucketname
	filename = bucketname + date + ".json"
	with open(filename, 'w') as target:
		myBucket = myClient.bucket(bucketname)
		writeBucket(myBucket, target)

# protocol to backup from multiple buckets into a new file
def multipleBucket(bucketList):
	print "all protocol"
	filename = "riak-backup-" + date + ".json"

	with open(filename, 'w') as target:
		start = time.time()
		for bucket in bucketList: 
			target.write('{"' + bucket + '": [')
			myBucket = myClient.bucket(bucket)
			#printKeys(myBucket)
			writeBucket(myBucket, target)
			target.write("]}\n")
		end = time.time()
		print "Total elapsed time is :" + str(end - start)

# function doesn't fully work, but supposed to return a generator of 
# keys from a bucket
def get_bucket_keys(bucket):
    for record_key in myClient.index(bucket, '$key', '0', 'Z').run():
        yield record_key

# used with the -a option
# returns the four buckets associated with the account passed in from command line
def createAccountBucketList():
	bucketList = []
	env = "dev" #change this later
	for types in typeList:
		bucketList.append(env + ".ps." + types + "." + str(args.account))
	return bucketList

if args.bucketname:
	bucketProtocol()
elif args.All:
	multipleBucket(getAccountsBuckets())
elif args.account:
	multipleBucket(createAccountBucketList())
elif args.print_keys:
	# for key in get_bucket_keys(args.print_keys):
	#     print key
	
	myBucket = myClient.bucket(args.print_keys)
	printKeys(myBucket)






#this search works but sometimes groups keys out of order and groups keys together
#in a way that doesn't make sense ... not sure why
	# for keys in bucket.stream_index('$key',0,'zzz'):
	# 	print keys
	# BELOW IS THE NON KEY VERIONS OF THIS. BOTH DO IT WEIRD	
	# stream = myClient.stream_keys(bucket)
	# for key_list in stream:
	#      print key_list
	# stream.close()	


# 	for key in myBucket.stream_index('eventTimestamp_bin',0,9999999999999):
# 		print key

	# for key in myClient.get_keys(bucket):
	# 	print key

