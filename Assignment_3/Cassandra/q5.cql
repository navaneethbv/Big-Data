/usr/local/apache-cassandra-2.0.5/bin/cqlsh csac0

cqlsh> CREATE KEYSPACE pxg142330 WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1};
cqlsh> USE nbv140130;
cqlsh:pxg142330> CREATE TABLE business3(business_id varchar PRIMARY KEY,full_address varchar,categories varchar);
cqlsh:pxg142330> copy business3(business_id,full_address,categories) from 'business.csv' with delimiter = '^';
cqlsh:pxg142330> select * from business where business_id = 'HPWmjuivv3xJ279qSVfNaQ';
cqlsh:pxg142330> truncate business;
cqlsh:pxg142330> drop table business;