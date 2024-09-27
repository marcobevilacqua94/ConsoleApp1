test code for bulk transactions in .net

use it with this arguments 

<host> <user> <password> <numDocs> <docSize> <expTime> <upsert>

localhost Administrator password 10000 100 300 false

<host> : couchbase host
<user> : couchbase user
<password> : couchbase password
<numDocs> : number of documents to upsert
<docSize> : size of documents to upsert
<expTime> : transaction expiration time in seconds
<upsert> : if true do upserts instead of inserts
