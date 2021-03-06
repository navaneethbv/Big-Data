-- Delete the file if it is already present
rmf $output_location 

-- Start of script
A = LOAD '/yelpdatafall/business/business.csv' USING PigStorage('^');
B = FOREACH A GENERATE $0 AS business_id,$1 AS full_address,$2 AS categories; 
K = FILTER B BY $1 MATCHES '.*CA.*';  

C = LOAD '/yelpdatafall/review/review.csv' USING PigStorage('^');
D = FOREACH C GENERATE $2 AS business_id,$3 AS rating;
E = GROUP D BY business_id;
F = FOREACH E GENERATE FLATTEN(group) AS business_id,AVG(D.rating) AS avg;

G = JOIN K BY business_id,F by business_id;
H = FOREACH G GENERATE $0 AS business_id,$1 AS full_address, $2 AS categories,$4 AS avg;
L = DISTINCT H;
I = ORDER L BY avg DESC;
J = LIMIT I 10;

STORE J INTO '$output_location' USING PigStorage('\t');
