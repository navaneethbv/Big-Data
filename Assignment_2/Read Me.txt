This Assignment was done by Navaneeth Venugopala Rao
Net ID - nbv140130

1. The first program is in Python. To execute this program copy the contents from Assignment2_1.py into the workspace and run the following command.

spark-submit Assignment2_1.py Stanford

2. Here Stanford is the address which is passed as a command line argument. The user can replace this with any other address.

3. Do not replace the indentations in the Python file as the code may not work. Please follow the given indentation if you are copy - pasting the code from Windows to Linux.

4. The second and the third program are in Scala.

5. To execute the Question 2A, start the spark-shell in the local mode by using the following command - 

spark-shell --master local[*]

6. Run the code under the Assignment2_2.scala and observe the run time.

7. To run the Assignment 2 in Yarn Mode, use the following command - 

spark-shell --master yarn-client --executor-memory 4G --executor-cores 7 --num-executors 6

8. The Execution Performance for the above modes is as follows -
-- Standalone Mode (Local Mode): 5357150 micro seconds
-- Yarn Mode 		          : 5327861 micro seconds

9. To execute the third program, start the spark-shell and then paste the code for 3A.

10. Note down the execution time.

11. Do the same thing for 3B and observe the execution time. We have used broadcast variable for 3B.

12. The Execution Performance for the above modes is as follows -
-- 3A (normal)		: 11180714 microseconds
-- 3B (broadcast) 	: 11072018 microseconds

13. Execution Time using Broadcast variable is faster.

