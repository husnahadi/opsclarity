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

start = time.time()
parser = argparse.ArgumentParser()
parser.add_argument("-A", "--All", action='store_true', help="backup the entire db")
parser.add_argument("-a", "--account", type=int, help="backup a particular account")
parser.add_argument("-b", "--bucketname", help="backup a particular bucket")
parser.add_argument("-p", "--print_keys", help="prints all of the keys for a bucket")
parser.add_argument("-t", "--time", type=int, help="restricts backup to certain range")
parser.add_argument("-f", "--restorefile", help="restore to a node from flat file")
parser.add_argument("-n", "--restorenode", action='store_true', help="restore to a node from node")
parser.add_argument("-s", "--SystemAll", action='store_true', help="does a thorough backup of all system buckets")
parser.add_argument("-d", "--delete", action='store_true', help="deletes unnecessary buckets")
args = parser.parse_args()

myClient = riak.RiakClient()
#myClient = riak.RiakClient(host='internal-staging-riak-private-1665852857.us-west-1.elb.amazonaws.com')
#print myClient.get_buckets()
date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
typeList = ['TimelineEvents', 'TopoVersions', 'RiakClient', 'AgentCache'] #RESOURCEVERSION
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
	env = "dev"
	for account in accountList:
		for types in typeList:
			bucketname = env + ".ps." + types + "." + str(account)
			bucket = myClient.bucket(bucketname)
			bucketList.append(bucket)
	#return bucketList
	return myClient.get_buckets()
	#return ['dev.ps.TopoVersions.541a2ac73730e63fe202edcb', 'unittest.ps.TimelineEvents.539b47353004433b6f35a699', 'unittest.ps.ResourceVersions.5403787a3c376271d86418ae', 'unittest.ps.ResourceVersionSnapshots.539b47353004433b6f35a699', 'dev.ps.TopoVersions.52df169fdc797880d43ebfa9', 'dev.ps.TopoVersions.53fb72af6fa37bb52fcc9463', 'unittest.ps.ResourceVersions.539b47353004433b6f35a699','unittest.ps.TimelineEvents.5565fff8d4c691a87d7df1f6', 'dev.ps.TopoVersions.52df169fdc797880d43ebfa9', 'unittest.ps.TimelineEvents.539b47353004433b6f35a699', 'dev.ps.TopoVersions.53e11d3283a26e8ceea1a79d', 'dev.ps.TopoVersions.532b446da4fd1e11bf89a35a', 'dev.ps.TopoVersions.52df1648dc797880d43ebfa5']

def writeBucket(bucket, target):
	"""
	writes data and indices from one bucket into target file
	@param bucket: Riak Bucket Object
	@param target: File 
	"""
	global keyCount
	totalReadfromNetwork = 0
	totalWritetoDisk = 0

	start = time.time()
	keys = getKeys(bucket)
	end = time.time()
	#print "Getting all keys time is " + str(end - start)

	# start = time.time()
	# objs = bucket.multiget(keys)

	# count = 0
	# for obj in objs:
	# 	count = count + 1
	# 	keyCount = keyCount + 1
	# 	#print "\n\nobj in loop is"
	# 	if type(obj) is riak.riak_object.RiakObject:
	# 		obj.indexes
	# end = time.time()
	# print "Multiget function time for " + str(count) + " keys is " + str(end - start)

	firstKey = True
	start = time.time()
	for key in keys: #one for each key
		keyCount = keyCount + 1
		s2 = time.time()
		dataObj = bucket.get(key)
		dataObjInd = dataObj.indexes
		# print dataObj
		# print dataObjInd
		e2 = time.time()
		totalReadfromNetwork = totalReadfromNetwork + (e2 - s2)

		# writing the data
		if not firstKey:
			target.write(", ")
		else:
			firstKey = False
		target.write("[")
		target.write(json.dumps(dataObj.data)) #(NO DECODER FOR TYPE APPLICATION/TEXT)
		target.write(' ,{ "indexes": [')
		
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
	#print "Writing data time is " + str(end - start)

	#print "totalReadfromNetwork time is " + str(totalReadfromNetwork)	

def writeBucketNode(bucket, client):
	"""
	writes data and indices from bucket into a new node
	@param bucket: Riak Bucket Object
	@param client: RiakClient to write new bucket to

	"""
	print "writing bucket to node"
	newBucket = client.bucket("NODE" + bucket.name)
	keys = getKeys(bucket)
	for key in keys:
		dataObj = bucket.get(key)
		data = json.dumps(bucket.get(key).data)
		newEntry = newBucket.new(key,data=dataObj.data)
		for index in dataObj.indexes:
			newEntry.add_index(index[0],index[1])
		newEntry.store()

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
	if args.restorenode:
		filename = bucketname + "-" + date + ".json"
		with open(filename, 'w') as target:
			writeBucket(myBucket, target)
	elif args.restorefile:
		writeBucketNode(myBucket, myClient)

def backupMultipleBucket(bucketList):
	"""
	used with either the --All or --account option
	protocol to backup data from multiple buckets into a new file
	@param bucketList: list of Riak Bucket Objects
	"""
	print "all protocol"
	if not args.restorenode and not args.delete:
		filename = "system-riak-backup-" + date + ".json"
		count = 1
		with open(filename, 'w') as target:
			for bucket in bucketList: 
				#print "\n ++ " + bucket.name + " #" + str(count)
				count = count + 1
				target.write('{"' + bucket.name + '": [')
				writeBucket(bucket, target)
				target.write("]}\n")
	elif args.restorenode:
		for bucket in bucketList:
			writeBucketNode(bucket, myClient)
	elif args.delete:
		print bucketList
		for bucket in bucketList:
			if bucket.name[:8] == "TESTTEST":
				print "Bucket to delete: " + bucket.name
				for key in getKeys(bucket):
					bucket.delete(key)

def restoreFromFileProtocol():
	"""
	used with the --restore option
	protocol to restore data from a JSON file (string passed from args.restore) into a node(s)
	"""
	print "restore protocol"
	with open(args.restorefile, 'r') as backup:
		for line in backup:
			bucketDict = json.loads(line)
			bucketname = bucketDict.keys()[0]
			#newBucket = myClient.bucket("TEST" + bucketname)
			newBucket = myClient.bucket(bucketname)			
			#print "\n# keys in " + newBucket.name + ": " + str(len(bucketDict[bucketname]))
			for data,indexes in bucketDict[bucketname]:
				#print data
				#print indexes
				key = data["id"]
				dataJSON = json.dumps(data)
				#print dataJSON
				newEntry = newBucket.new(key,data=data)
				#result = newBucket.get(key)
				#print result.data
				print "printing indexes"
				for index in indexes[indexes.keys()[0]]:
					idx = index.keys()[0].encode('ascii','ignore')
					val = index[index.keys()[0]]
					newEntry.add_index(idx,val)
				newEntry.store()
	myClient.get_buckets()

def createAccountBucketList():
	"""
	used with the -account option
	@return: list of strings representing the buckets associated with the account (string passed from args.account)
	"""	
	bucketList = []
	env = "dev" #change this later
	for types in typeList:
		bucketList.append(env + ".ps." + types + "." + str(args.account))
	return bucketList

def printKeys(bucket):
	"""
	used with the -printkeys option
	prints all of the keys in a bucket (for debugging purposes)
	@param bucket: Riak Bucket Object
	"""
	for keys in bucket.get_index('$key','0','Z'):
		print keys

	# for key in myClient.get_keys(bucket): #slower
	# 	print key

	# for keys in bucket.stream_index('$key',0,'zzz', return_terms=True): #not actually faster
	# 	print keys	

#parses arguments to follow code execution
if args.bucketname:
	backupSingleBucketProtocol()
elif args.All:
	backupMultipleBucket(getAccountsBuckets())
elif args.account:
	backupMultipleBucket(createAccountBucketList())
elif args.print_keys:
	myBucket = myClient.bucket(args.print_keys)
	printKeys(myBucket)
elif args.restorefile:
	restoreFromFileProtocol()
elif args.SystemAll:
	print "Getting bucketList"
	#stagingClient = riak.RiakClient(host='internal-staging-riak-private-1665852857.us-west-1.elb.amazonaws.com')
	#bucketList = stagingClient.get_buckets()
	bucketList = myClient.get_buckets()
	print "Starting writing buckets"
	backupMultipleBucket(bucketList)
end = time.time()
print "Total program time for " + str(keyCount) + " keys is " + str(end-start)


# def get_bucket_keys(bucket):
# 	"""
# 	function doesn't fully work, but supposed to return a generator of 
# 	keys from a bucket	
#     """
#     for record_key in myClient.index(bucket, '$key', '0', 'Z').run():
#         yield record_key
# for key in get_bucket_keys(args.print_keys):
# print key

#this search works but sometimes groups keys out of order and groups keys together
#in a way that doesn't make sense ... not sure why
	# for keys in bucket.stream_index('$key',0,'zzz'):
	# 	print keys
	# BELOW IS THE NON KEY VERIONS OF THIS. BOTH DO IT WEIRD	
	# stream = myClient.stream_keys(bucket)
	# for key_list in stream:
	#      print key_list
	# stream.close()

