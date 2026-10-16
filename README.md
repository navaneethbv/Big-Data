# Big Data coursework

Historical Hadoop, Spark, Pig, Cassandra, and clustering assignments.
Source, reports, and captured results are preserved together.

## Assignment map

| Folder | Contents |
| --- | --- |
| [Assignment 1](Assignment_1/Big%20Data%20Assignment/README.md) | Java Hadoop MapReduce queries and joins over business/review data. |
| [Assignment 2](Assignment_2/README.md) | PySpark business lookup and Scala join/broadcast experiments. |
| [Assignment 3](Assignment_3/README.md) | Pig queries and Cassandra schema/data exercises. |
| [Assignment 3B](Assignment_3B/) | Written solution and original assignment material. |
| [Assignment 4](Assignment_4/README.txt) | Scala Spark clustering/recommendation exercises and captured results. |

## Reproducing the assignments

Read the corresponding assignment guide and source before running a command.
Paths such as `/yelpdata`, university hosts, Windows Spark directories, and cluster output locations describe the original environment.
Replace those paths with your own input/output locations, including hard-coded paths in source.
The Spark 1.5.1 and Cassandra 2.0.5 references are historical versions, not instructions to install the latest release.
There is no repository-wide dependency lockfile or automated test suite establishing compatibility with current cluster software.
Reported timings are original experiment results and are not current performance guarantees.
