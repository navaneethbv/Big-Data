
-- 3 A))
val data = sc.textFile("/yelpdatafall/review/review.csv")
val data1 = data.map(line => line.split("\\^")).map(line => (line(2), line(3).toDouble))
val result =  data1.map { case (key, value) => (key, (value, 1)) }.reduceByKey { case ((value1, count1), (value2, count2)) => (value1 + value2, count1 + count2)}.mapValues {case (value, count) =>  value.toDouble / count.toDouble}
val topTen = result.takeOrdered(10)(Ordering[Double].reverse.on(x=>x._2))
val topTen2 = sc.parallelize(topTen) 
   
val data2 = sc.textFile("/yelpdatafall/business/business.csv")
val data3 = data2.map(line => line.split("\\^")).map(line => (line(0), (line(1)+"\t\t"+line(2)).toString))

val res = data3.join(topTen2).distinct()
res.foreach(println)

-- 3 B))
val data = sc.textFile("/yelpdatafall/review/review.csv")
val data1 = data.map(line => line.split("\\^")).map(line => (line(2), line(3).toDouble))
val result =  data1.map { case (key, value) => (key, (value, 1)) }.reduceByKey { case ((value1, count1), (value2, count2)) => (value1 + value2, count1 + count2)}.mapValues {case (value, count) =>  value.toDouble / count.toDouble}
val topTen = result.takeOrdered(10)(Ordering[Double].reverse.on(x=>x._2))
val topTen2 = sc.broadcast(topTen)

val data2 = sc.textFile("/yelpdatafall/business/business.csv")
val data3 = data2.map(line => line.split("\\^")).map(line => (line(0), (line(1)+"\t\t"+line(2)).toString))

val topTen3 = sc.parallelize(topTen2)
val res = data3.join(topTen3).distinct()
res.foreach(println)



-- Execution Performance
-- 3A (normal)		: 11180714 microseconds
-- 3B (broadcast) 	: 11072018 microseconds

-- Execution Time using Broadcast variable is faster.
