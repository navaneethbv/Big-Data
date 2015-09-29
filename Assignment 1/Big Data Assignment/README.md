This assignment was done by Navaneeth Venugopala Rao
Utd Id - nbv140130

How To Run - Problem 1
--------------------------------------------------------------------
Use following command to run 

$ hadoop jar ListBusinessId.jar ListBusinessId /yelpdata/business.csv /nbv140130/yq1


input file: /yelpdata/business.csv 
Output directory: /nbv140130/yq1

----------------------------------------------------------------------

Output of the program is there in a "Output" file. You can use Notepad++ to open the
same file to see the output.

---------------------------------------------------------------------
Folder contains
.java files --->Source Code
.jar file
Output file
ReadMe

How To Run - Problem 2
--------------------------------------------------------------------
Use following command to run 

$ hadoop jar TopTenBusiness.jar TopTenBusiness /yelpdata/business.csv /nbv140130/yq2


input file: /yelpdata/business.csv 
Output directory: /nbv140130/yq2

----------------------------------------------------------------------

Output of the program is there in a "Output" file. You can use Notepad++ to open the
same file to see the output.

---------------------------------------------------------------------
Folder contains
.java files --->Source Code
.jar file
Output file
ReadMe

How To Run - Problem 3
--------------------------------------------------------------------
Use following command to run (on local machine...)

$ hadoop jar ReduceSideJoin.jar ReduceSideJoin /yelpdata/review.csv /yelpdata/business.csv yelpdata/output4i /nbv140130/yq4

Intermediate file: output4i
input file: /yelpdata/review.csv /yelpdata/business.csv
Output directory: /nbv140130/yq4

----------------------------------------------------------------------

Output of the program is there in a "Output" file. You can use Notepad++ to open the
same file to see the output.

---------------------------------------------------------------------
Folder contains
.java files --->Source Code
.jar file
Output file
ReadMe


How To Run - Problem 4
--------------------------------------------------------------------
Use following command to run (on local machine...)

Note : This problem has been run on the local cluster.

$ hadoop jar MapSideJoin.jar MapSideJoin /yelpdata/business.csv /yelpdata/review.csv yelpdata/output4i /nbv140130/yq4

Intermediate file: output4i
input file: /yelpdata/business.csv /yelpdata/review.csv
Output directory: /nbv140130/yq4

----------------------------------------------------------------------

Output of the program is there in a "Output" file. You can use Notepad++ to open the
same file to see the output.

---------------------------------------------------------------------
Folder contains
.java files --->Source Code
.jar file
Output file
ReadMe


Note : To run the map side reduce on the college cluster, the execution remains same but the cache part has been hardcoded in the program. This code is present inside Source Code/Problem 4 - College Cluster
