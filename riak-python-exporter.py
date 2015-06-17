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
parser.add_argument("-a", "--account", help="backup a particular account")
parser.add_argument("-b", "--bucketname", help="backup a particular bucket")
parser.add_argument("-t", "--time", type=int, help="restricts backup to certain number of days ago")
parser.add_argument("-f", "--restorefile", help="restore to a node from flat file")
parser.add_argument("-n", "--restorenode", action='store_true', help="restore to a node from node")
parser.add_argument("-d", "--delete", action='store_true', help="deletes unnecessary buckets")
parser.add_argument("-s", "--server", help="which server to back it up from")
parser.add_argument("-T", "--topoversion", action = 'store_true', help="backups topoversion types")
parser.add_argument("-E", "--events", action='store_true', help='backups larger buckets, TimelineEvents and ResourceVersions')
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
elif args.server in ["staging", "prod", "nightly"]:
	hostDict = {"staging": "internal-staging-riak-private-1665852857.us-west-1.elb.amazonaws.com", "prod": "internal-prod-riak-840856407.us-west-1.elb.amazonaws.com", "nightly": "nightly-platform.prod.opsclarity.com"}
	myClient = riak.RiakClient(host=hostDict[args.server], protocol=protocol)
elif not args.server:
	if not args.restorefile and not args.restorenode:
		print "Error: required server name (using -s option)"
		sys.exit(0)
else:
	print "Error: incorrect server name"
	sys.exit(0)
#print myClient.get_buckets()
localClient = riak.RiakClient(protocol="http")
#date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M')
date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
keyCount = 0

def getAccountsBuckets():
	"""
	used with the -All option
	returns list of all buckets in the system using account names from local mongodb	
	must have mongoDB client running in background before program starts
	@return: list of Riak Bucket Objects
	"""
	# access mongodb to get list of accounts
	db = MongoClient().configdb_dev
	accountList = []
	for d in db.Accounts.find({},{"_id":True}):
		accountList.append(d["_id"])

	bucketList = []
	env = args.server
	for account in accountList:
		for types in typeList:
			bucketname = env + ".ps." + types + "." + str(account)
			bucket = myClient.bucket(bucketname)
			bucketList.append(bucket)
	return bucketList
	
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
	firstKey = True
	for dataObj in dataObjs: #one for each key
		keyCount += 1
		data = dataObj.encoded_data

		# writing the data
		if not firstKey:
			target.write(", ")
		else:
			firstKey = False

		if data:
			target.write('[{"key": "' + str(dataObj.key) + '"}, ')
			target.write(data)
			target.write(' , { "indexes": [')
			
			# writing the data's indices
			firstIdx = True
			dataObjInd = dataObj.indexes
			if dataObjInd:
				for idx,val in dataObjInd:
					if not firstIdx:
						target.write(", ")
					else:
						firstIdx = False
					target.write(json.dumps({idx:val}))
			target.write("]} ]")

	end = time.time()
	print "Backing up bucket total is " + str(end - start1)

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
	keyCount += len(keys)
	start = time.time()
	dataObjs = bucket.multiget(keys)
	end = time.time()
	print "Getting dataObjs time is " + str(end - start)

	newEntry_time = 0
	store_newEntry_time = 0
	count = 0

	for dataObj in dataObjs:
		count += 1
		start = time.time()
		dataObj = dataObj.data
		if dDataObj:
			newEntry = newBucket.new(dataObj.key, data=dataObj)
			end = time.time()
			newEntry_time = newEntry_time + (end - start)

			indexes = dataObj.indexes
			if indexes:
				for index in indexes:
					newEntry.add_index(index[0],index[1])

			start = time.time()
			newEntry.store(return_body=False)
			end = time.time()
			store_newEntry_time = store_newEntry_time + (end - start)

	end = time.time()

	print "Creating newEntry time is " + str(newEntry_time)
	print "Storing newEntry time is " + str(store_newEntry_time)	
	print "Restoring up bucket total is " + str(end - start0) + "\n"

def getKeys(bucket):
	"""
	@param bucket: Riak Bucket Object
	@return: list of relevant keys for the bucket, depending on if --time option is set
	"""	
	if args.time:
		startTime = int(time.time() - args.time*24*60*60)
		bucketTimeDict = {'TimeLineEvents':'eventtimestamp_int','ResourceVersions':'timestamp_int'}
		bucketType = bucket.name.split('.')[2]
		if bucketType == 'TimelineEvents' or bucketType == 'ResourceVersions':
			return bucket.get_index(bucketTimeDict[bucketType], startTime, 99999999999999)
		else:
			print "Filtering by time for bucket " + str(bucket.name) + " not supported"
			sys.exit(0)

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
		if args.All:
			filename = args.server + "-riak-backup-" + date + ".json"
		elif args.account:
			filename = "account-" + str(args.account) + "-riak-backup-" + date + ".json"
		print filename
		count = 1
		with open(filename, 'w') as target:
			for bucket in bucketList: 
				print "\n ++ " + bucket.name + " #" + str(count)
				count = count + 1
				if bucket.name[:27] != "staging.ps.LiveTopoVersions" and bucket.name[:7] != "mohamed" and bucket.name[:27] != "nightly.ps.LiveTopoVersions" and bucket.name[:24] != "prod.ps.LiveTopoVersions": #content type application/text error if removed
					target.write('{"' + bucket.name + '": [')
					writeBucket(bucket, target)
					target.write("]}\n")
	elif args.restorenode:
		count = 1
		for bucket in bucketList:
			print "\n ++ " + bucket.name + " #" + str(count)
			count += 1
			if bucket.name[:27] != "staging.ps.LiveTopoVersions" and bucket.name[:7] != "mohamed":
				writeBucketNode(bucket, localClient)
	elif args.delete:
		#print bucketList
		for bucket in bucketList:
			print "Bucket to delete: " + bucket.name
			keys = getKeys(bucket)
			global keyCount 
			keyCount += len(keys)
			for key in keys:
				bucket.delete(key)

def restoreFromFileProtocol():
	"""
	used with the --restore option
	protocol to restore data from a JSON file (string passed from args.restore) into a node(s)
	"""
	global keyCount 
	print "Restore Protocol"
	with open(args.restorefile, 'r') as backup:
		count = 0
		for line in backup:
			count += 1
			print "Bucket #" + str(count)

			start = time.time()
			bucketDict = json.loads(line)
			end = time.time()
			print "Loading json time is " + str(end-start)

			bucketname = bucketDict.keys()[0]
			print "++ " + str(bucketname) + " #" + str(count)
			newBucket = myClient.bucket(bucketname)			
			print "# keys in " + newBucket.name + ": " + str(len(bucketDict[bucketname]))
			keyCount += len(bucketDict[bucketname])
			store_newEntry_time = 0
			start = time.time()
			for key, data,indexes in bucketDict[bucketname]:
				newEntry = newBucket.new(key["key"],data=data)
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
			print "Time to restore all bucket is " + str(end - start)

def createAccountBucketList():
	"""
	used with the -account option
	generates a list of Riak buckets associated with the account passed in from args.account
	@return: list of Riak Bucket Objects
	"""	
	buckets = myClient.get_buckets()
	bucketList = []
	if args.server == "local":
		env = "dev"
	else:
		env = args.server

	def appendBucket(bucketType):
		bucketString = env + ".ps." + bucketType + "." + str(args.account)
		bucket = myClient.bucket(bucketString)
		if bucket in buckets:
			bucketList.append(bucket)
		else:
			print "Error: Bucket " + bucketString + " cannot be found"
			sys.exit(0)

	if args.topoversion:
		appendBucket("TopoVersions")		
	if args.events:
		appendBucket("ResourceVersions")
		appendBucket("TimelineEvents")
	return bucketList

#parses arguments to follow code execution
if args.bucketname:
	backupSingleBucketProtocol()
elif args.All:
	backupMultipleBucket(myClient.get_buckets())
elif args.account:
	if args.account == "opsify":
		args.account = "52df1648dc797880d43ebfa5"
	backupMultipleBucket(createAccountBucketList())
elif args.restorefile:
	restoreFromFileProtocol()
end = time.time()
print "Total program time for " + str(keyCount) + " keys is " + str(end-start)
