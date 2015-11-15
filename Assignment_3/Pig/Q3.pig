-- Delete the file if already present 
rmf $output_location

-- Start of script 

A = LOAD '/yelpdatafall/business/business.csv' USING PigStorage('^');
B = LOAD '/yelpdatafall/review/review.csv' USING PigStorage('^');
C = COGROUP A BY $0 INNER, B BY $2 INNER;
D = DISTINCT C;
E = LIMIT D 5;

STORE E INTO '$output_location' USING PigStorage('\t');

 
