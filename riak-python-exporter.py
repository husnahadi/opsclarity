from sys import argv
import argparse
import time
import datetime
import riak
try:
	import simplejson as json
except ImportError:
	import json
from pymongo import MongoClient
import threading
import itertools
import sys

start = time.time()
parser = argparse.ArgumentParser()
parser.add_argument("-A", "--All", action='store_true', help="backup the entire db")
parser.add_argument("-a", "--account", type=int, help="backup a particular account")
parser.add_argument("-b", "--bucketname", help="backup a particular bucket")
parser.add_argument("-t", "--time", type=int, help="restricts backup to certain range")
parser.add_argument("-f", "--restorefile", help="restore to a node from flat file")
parser.add_argument("-n", "--restorenode", action='store_true', help="restore to a node from node")
parser.add_argument("-d", "--delete", action='store_true', help="deletes unnecessary buckets")
parser.add_argument("-s", "--server", help="which server to back it up from")
args = parser.parse_args()
myClient = []
protocol = ""
if args.restorenode or args.restorefile:
	protocol = "http"
else:
	protocol = "pbc"
print "Protocol type: " + protocol

if args.server == "local":
	myClient = riak.RiakClient(protocol=protocol)
elif args.server == "staging":
	myClient = riak.RiakClient(host='internal-staging-riak-private-1665852857.us-west-1.elb.amazonaws.com', protocol=protocol)
elif args.server == "prod":
	myClient == riak.RiakClient(host='internal-prod-riak-840856407.us-west-1.elb.amazonaws.com', protocol = protocol)
elif args.server == "nightly":
	myClient == riak.RiakClient(host='nightly-platform.prod.opsclarity.com', protocol = protocol)
elif not args.server:
	print "Error: required server name (using -s option)"
	sys.exit(0)
else:
	print "Error: incorrect server name"
	sys.exit(0)
localClient = riak.RiakClient(protocol="http")
date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M')
typeList = ['TimelineEvents', 'TopoVersions', 'RiakClient', 'AgentCache'] #'ResourceVersion'
keyCount = 0

def getAccountsBuckets():
	"""
	used with the -All option
	returns list of all buckets in the system using account names from local mongodb	
	must have mongoDB client running in background before program starts
	@return: list of Riak Bucket Objects
	"""
	# UNCOMMENT WHEN TALKED TO NARA:
	# # access mongodb to get list of accounts
	# db = MongoClient().configdb_dev
	# accountList = []
	# for d in db.Accounts.find({},{"_id":True}):
	# 	accountList.append(d["_id"])

	# bucketList = []
	# env = "dev"
	# for account in accountList:
	# 	for types in typeList:
	# 		bucketname = env + ".ps." + types + "." + str(account)
	# 		bucket = myClient.bucket(bucketname)
	# 		bucketList.append(bucket)
	# return bucketList

	start = time.time()
	buckets = myClient.get_buckets()
	end = time.time()
	print "time to retrieve list of buckets is " + str(end - start)
	return buckets
	
def writeBucket(bucket, target):
	"""
	writes data and indices from one bucket into target file
	@param bucket: Riak Bucket Object
	@param target: File 
	"""
	global keyCount

	start1 = time.time()

	start = time.time()
	keys = getKeys(bucket)
	end = time.time()
	print "Getting all " + str(len(keys)) + " keys time is " + str(end - start)

	start = time.time()
	dataObjs = bucket.multiget(keys)
	end = time.time()
	print "Multiget function time is " + str(end - start)

	dataObj_time = 0
	json_time = 0	
	writing_time = 0
	index_time = 0
	firstKey = True
	for dataObj in dataObjs: #one for each key
		keyCount = keyCount + 1
		s2 = time.time()
		dataObjInd = dataObj.indexes
		e2 = time.time()
		index_time = index_time + (e2 - s2)

		# writing the data
		if not firstKey:
			target.write(", ")
		else:
			firstKey = False

		s3 = time.time()
		x = dataObj.encoded_data
		e3 = time.time()
		dataObj_time = dataObj_time + (e3 - s3)

		if x:
			target.write("[")
			target.write(x)
			target.write(' , { "indexes": [')
			
			# writing the data's indices
			firstIdx = True
			if dataObjInd:
				for idx,val in dataObjInd:
					if not firstIdx:
						target.write(", ")
					else:
						firstIdx = False
					target.write(json.dumps({idx:val}))
			target.write("]} ]")

	end = time.time()
	print "Accessing index time is " + str(index_time)
	print "Creating data object time is " + str(dataObj_time)
	print "Creating json time is " + str(json_time)
	print "Backing up bucket total is " + str(end - start1)

	#print "totalReadfromNetwork time is " + str(totalReadfromNetwork)	

def writeBucketNode(bucket, client):
	"""
	writes data and indices from bucket into a new node
	@param bucket: Riak Bucket Object
	@param client: RiakClient to write new bucket to

	"""
	global keyCount
	start0 = time.time()
	newBucket = client.bucket(bucket.name)
	start = time.time()
	keys = getKeys(bucket)
	end = time.time()
	print "Getting keys time for " + str(len(keys)) + " keys is " + str(end - start)
	keyCount = keyCount + len(keys)
	start = time.time()
	dataObjs = bucket.multiget(keys)
	end = time.time()
	print "Getting dataObjs time is " + str(end - start)

	index_time = 0
	newEntry_time = 0
	store_newEntry_time = 0
	count = 0

	for dataObj in dataObjs:
		count = count + 1
		start = time.time()
		# if dataObj.key != "55687b61e4b0c65d2a8f46db" and dataObj.key != "55686646e4b0c65d2a8a0f91" and dataObj.key != "55688d71e4b0b144746b8642" and dataObj.key != "55689479e4b0b144746e9fd6": #encoding unicode issue
		# 	newEntry = newBucket.new(dataObj.key, data=dataObj.encoded_data)
		# else:
		# 	newEntry = newEntry = newBucket.new(dataObj.key, data=dataObj.data)
		dDataObj = dataObj.data
		if dDataObj:
			newEntry = newBucket.new(dataObj.key, data=dDataObj)
			end = time.time()
			newEntry_time = newEntry_time + (end - start)

			start1 = time.time()
			if dataObj.indexes:
				for index in dataObj.indexes:
					newEntry.add_index(index[0],index[1])
			start2 = time.time()
			index_time = index_time + (end - start)

			start = time.time()
			newEntry.store(return_body=False)
			end = time.time()
			store_newEntry_time = store_newEntry_time + (end - start)

	end = time.time()

	print "Creating index time is " + str(index_time)
	print "Creating newEntry time is " + str(newEntry_time)
	print "Storing newEntry time is " + str(store_newEntry_time)	
	print "Restoring up bucket total is " + str(end - start0) + "\n"

def getKeys(bucket):
	"""
	@param bucket: Riak Bucket Object
	@return: list of relevant keys for the bucket, depending on if --time option is set
	"""	
	if args.time:
		return bucket.get_index('timestamp_int', args.time, 99999999999999)
	else:
		#myClient.get_keys(bucket)
		return bucket.get_index('$key','0','Z')	#return result could be HUGE. stream it with multiple loops perhaps. doesn't work with my bucket test

def backupSingleBucketProtocol():
	"""
	used with the -bucket option
	backups data from a single bucket (string passed from args.bucketname) into a new file
	"""
	print "bucket protocol"
	bucketname = args.bucketname
	myBucket = myClient.bucket(bucketname)
	if not args.restorenode:
		filename = bucketname + "-" + date + ".json"
		with open(filename, 'w') as target:
			target.write('{"' + bucketname + '": [')
			writeBucket(myBucket, target)
			target.write("]}\n")
	else:
		writeBucketNode(myBucket, localClient)

def backupMultipleBucket(bucketList):
	"""
	used with either the --All or --account option
	protocol to backup data from multiple buckets into a new file
	@param bucketList: list of Riak Bucket Objects
	"""
	print "all protocol"
	if not args.restorenode and not args.delete:
		filename = args.server + "-riak-backup-" + date + ".json"
		count = 1
		with open(filename, 'w') as target:
			for bucket in bucketList: 
				print "\n ++ " + bucket.name + " #" + str(count)
				count = count + 1
				if bucket.name[:27] != "staging.ps.LiveTopoVersions" and bucket.name[:7] != "mohamed": #content type application/text error if removed
					target.write('{"' + bucket.name + '": [')
					writeBucket(bucket, target)
					target.write("]}\n")
	elif args.restorenode:
		count = 1
		for bucket in bucketList:
			print "\n ++ " + bucket.name + " #" + str(count)
			count = count + 1
			if bucket.name[:27] != "staging.ps.LiveTopoVersions" and bucket.name[:7] != "mohamed":
				writeBucketNode(bucket, localClient)
	elif args.delete:
		#print bucketList
		for bucket in bucketList:
			print "Bucket to delete: " + bucket.name
			keys = getKeys(bucket)
			global keyCount 
			keyCount = keyCount + len(keys)
			for key in keys:
				bucket.delete(key)

def restoreFromFileProtocol():
	"""
	used with the --restore option
	protocol to restore data from a JSON file (string passed from args.restore) into a node(s)
	"""
	global keyCount 
	print "restore protocol"
	with open(args.restorefile, 'r') as backup:
		count = 1
		for line in backup:
			count = count + 1
			print "Bucket #" + str(count)
			start = time.time()
			bucketDict = json.loads(line)

			end = time.time()
			print "Loading json time is " + str(end-start)
			bucketname = bucketDict.keys()[0]

			print "++ " + str(bucketname) + " #" + str(count)
			if bucketname[:22] != "dev.ui.userPreferences" and bucketname[:26] != "staging.ui.userPreferences" and bucketname[:24] != "local.ui.userPreferences" and bucketname[:31] != "all.providence.accountsummaries" and bucketname[:18] != "dev.ps.AgentCaches" and bucketname[:21] != "ds.ui.userPreferences" and bucketname[:23] != "prod.ui.userPreferences" and bucketname != "staging.ps.AgentCaches." and bucketname != "all.providence.awsInfo" and bucketname != "graphs" and bucketname != "staging.aws.CloudWatchMetrics":
				newBucket = myClient.bucket(bucketname)			
				print "# keys in " + newBucket.name + ": " + str(len(bucketDict[bucketname]))
				keyCount = keyCount + len(bucketDict[bucketname])
				store_newEntry_time = 0
				start = time.time()
				for data,indexes in bucketDict[bucketname]:
					key = data["id"]
					newEntry = newBucket.new(key,data=data)
					for index in indexes[indexes.keys()[0]]:
						idx = index.keys()[0].encode('ascii','ignore')
						val = index[index.keys()[0]]
						newEntry.add_index(idx,val)
					s2 = time.time()
					newEntry.store(return_body=False)
					e2 = time.time()
					store_newEntry_time = store_newEntry_time + (e2-s2)
				end = time.time()
				print "Time to store keys is " + str(store_newEntry_time)
				print "Time to write all keys is " + str(end - start)
			print "Bucket done\n"

def createAccountBucketList():
	"""
	used with the -account option
	@return: list of strings representing the buckets associated with the account (string passed from args.account)
	"""	
	bucketList = []
	env = "dev" #change l8r
	for types in typeList:
		bucketList.append(env + ".ps." + types + "." + str(args.account))
	return bucketList

#parses arguments to follow code execution
if args.bucketname:
	backupSingleBucketProtocol()
elif args.All:
	backupMultipleBucket(getAccountsBuckets())
elif args.account:
	backupMultipleBucket(createAccountBucketList())
elif args.restorefile:
	restoreFromFileProtocol()
end = time.time()
print "Total program time for " + str(keyCount) + " keys is " + str(end-start)
