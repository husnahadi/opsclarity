#riak-backup.py

python riak-backup.py [options]

note for -All option, must have mongod running on the local host
other stuff to install: pymongo (pip install pymongo), riak python client
Ignore warning about OpenSSL (packaging issue with python riak client)
Output backup file generated is >1.5 GB for staging server.

**options:**
	-h, --help					output usage information
	-s, --server				required. specifies which riak server to backup from. options: local or staging	
	-A, --All					backup all buckets in specified server
	-a, --account [accountID]	backup all buckets associated with [accountID]
	-b, --bucket [bucket]		backup all data in [bucket]
	-t, --time [time]			restricts backup to everything after [time]
	-f, --restorefile [file]	backups from particular file into a riak instance
	-n, --restorenode			changes default of writing backupts to json file, to writing to another riak instance (default is local riak instance)
	-d, --delete				if option is set with -A option, deletes every bucket (only use if sure you want to delete). -n and -d must not be set in same command

**Example Use:**

To backup all data from staging server to a file called staging-riak-backup-TODAYS_DATE.json:
python riak-backup.py -A -s staging

To restore your local riak from a backup file (hardcoded riak to write to is the local riak):
python riak-backupy.py -f staging-riak-backup-TODAYS_DATE.json

To write from a riak server directly into the local riak node:
python riak-backup.py -An -s staging

Note, data must be in json format, encapsulated in '{}' braces for restore to work properly.

Exits if no bucket found