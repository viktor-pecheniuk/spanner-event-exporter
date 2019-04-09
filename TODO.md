Spez Todo
=========

 - Add proper logging
 - Add bootstrap components to get last processed timestamp
 - Need to strip out size from the type object and add it to the avro datatype i.e
   i.e STRING(MAX) vs STRING(1024)
   Fixed length strings will be unsupported in the first release.
 - Add Checkstyle config via uri: (is currently causing an error)
 ```groovy
   checkstyle {
    config project.resources.text.fromUri('${https://raw.githubusercontent.com/googleapis/google-cloud-java/master}/checkstyle.xml')
  }
```
 - Document creating a spez Queue table without the use of the
   Queue.createTable() helper.
``` sql
CREATE TABLE spez_poller_table_queue (
    MessageId STRING(36) NOT NULL,
    Ack BOOL NOT NULL,
    Key STRING(1024) NOT NULL,
    Payload BYTES(MAX) NOT NULL,
    Timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (MessageId)
```
