var movie = readLine("Enter movie id\n")
var movieid = movie.toInt  
var data = sc.textFile("itemusermat")
//split the parts into individual numbers   
var mapped = data.map(line => line.split(" ")) 
//convert the numbers from String to Int   
var intMap = mapped.map(x => x.map(_.toInt)) 
import java.util.Arrays 
//set 0th column as key and rest as array of values
var kv = intMap.map(x => (x(0), Arrays.copyOfRange(x, 1,6041)))
//save the key value map into array from RDD


//key,value <1718, [0,0,0,0,0,0,0,0]>
//take input from user for a movie ID

// have 1 for loop = x for given movie and y = 1 to N 
//similarity(x,y) calculation
// <1718, [-1.5,2.3,0,-1.2]>
// multiply each item of given movie with current y. And add the products together.
//THIS IS YOUR NUMERATOR
// For denominator, square each element in the row and add the squares. Take sqrt of the sum. Then multiply the two roots

//calculate mean for every row and save in Map <1718,2.5>
var means = kv.map(x=> (x._1,x._2.sum)).map(x=>(x._1,x._2/6040.0))
var joined = kv.join(means)
var newrow = joined.map(x => (x._1, x._2._1.toList.map(y=> y - x._2._2)))
var xrow = newrow.filter(x=> x._1 == movieid).collect()(0)._2


//numerator
var mult = newrow.map(x=> (x._1, (for((a,b) <- x._2 zip xrow) yield a*b) sum))

//denominator part 1
var rx = newrow.filter(x=> x._1 == movieid).mapValues(x => x.toList.map(y=>y*y)).collect()(0)._2.sum
var sqrtrx = scala.math.sqrt(rx)
var ry = newrow.mapValues(x => x.toList.map(y=>y*y)).map(y=> (y._1,y._2.sum)).mapValues(z => scala.math.sqrt(z)).mapValues(w => w*sqrtrx)

var eqn = mult.join(ry)
var res = eqn.mapValues(x=> x._1/x._2)

var top5 = res.map{case(a,b) => (b,a)}.sortByKey(false).map{case(a,b) => (b,a)}.take(6).drop(1)
var top5RDD = sc.parallelize(top5)
var moviedata = sc.textFile("movies.dat")
var moviesplit = moviedata.map(line=> line.split("::")).map(part => (part(0).toInt,(part(1),part(2))))
var solution = top5RDD.leftOuterJoin(moviesplit) 
var finalRDD = solution.map(x => (x._1, x._2._2))
var finalAns = finalRDD.collect()

