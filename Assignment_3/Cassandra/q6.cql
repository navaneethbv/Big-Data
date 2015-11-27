/usr/local/apache-cassandra-2.0.5/bin/cqlsh csac0

cqlsh> CREATE KEYSPACE nbv140130 WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1};
cqlsh> USE nbv140130;
cqlsh:nbv140130> CREATE TABLE review(review_id varchar, user_id varchar ,business_id varchar,rating float, PRIMARY KEY((business_id),rating)) WITH CLUSTERING ORDER BY (rating DESC);
cqlsh:nbv140130> CREATE INDEX stars_index ON review (rating);
cqlsh:nbv140130> COPY review(review_id,user_id,business_id,rating) from 'review.csv' with delimiter = '^';
cqlsh:nbv140130> select * from review where rating = 4.0 LIMIT 10;
cqlsh:nbv140130> truncate review;
cqlsh:nbv140130> drop table review;
