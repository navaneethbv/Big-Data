/usr/local/apache-cassandra-2.0.5/bin/cqlsh csac0

cqlsh> CREATE KEYSPACE nbv140130 WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1};
cqlsh> USE nbv140130;
cqlsh:nbv140130> CREATE TABLE business3(business_id varchar PRIMARY KEY,full_address varchar,categories varchar);
cqlsh:nbv140130> copy business3(business_id,full_address,categories) from 'business.csv' with delimiter = '^';
cqlsh:nbv140130> select * from business where business_id = 'HPWmjuivv3xJ279qSVfNaQ';
cqlsh:nbv140130> truncate business;
cqlsh:nbv140130> drop table business;