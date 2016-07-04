import breeze.linalg._ 
import breeze.linalg._ 
import org.apache.spark.HashPartitioner 

val ratings = sc.textFile("hdfs://cshadoop1.utdallas.edu/hw4fall/ratings.dat").map(l => (l.split("::")(0),l.split("::")(1),l.split("::")(2))) 
val itemCount = ratings.map(x=>x._2).distinct.count 
val userCount = ratings.map(x=>x._1).distinct.count 
val items = ratings.map(x=>x._2).distinct   
val users = ratings.map(x=>x._1).distinct  
val k= 5   
val itemMatrix = items.map(x=> (x,DenseVector.zeros[Double](k)))  
var myitemMatrix = itemMatrix.map(x => (x._1,x._2(0 to k-1):=0.5)).partitionBy(new HashPartitioner(10)).persist  
val userMatrix = users.map(x=> (x,DenseVector.zeros[Double](k)))
var myuserMatrix = userMatrix.map(x => (x._1,x._2(0 to k-1):=0.5)).partitionBy(new HashPartitioner(10)).persist
val ratingByItem = sc.broadcast(ratings.map(x => (x._2,(x._1,x._3)))) 
val ratingByUser = sc.broadcast(ratings.map(x => (x._1,(x._2,x._3)))) 
var i =0
for( i <- 1 to 10){ 

	val ratItemVec = myitemMatrix.join(ratingByItem.value)
	val regfactor = 1.0 
	val regMatrix = DenseMatrix.zeros[Double](k,k)  
	regMatrix(0,::) := DenseVector(regfactor,0,0,0,0).t 
	regMatrix(1,::) := DenseVector(0,regfactor,0,0,0).t 
	regMatrix(2,::) := DenseVector(0,0,regfactor,0,0).t 
	regMatrix(3,::) := DenseVector(0,0,0,regfactor,0).t 
	regMatrix(4,::) := DenseVector(0,0,0,0,regfactor).t
	val userbyItemMat = ratItemVec.map(x => (x._2._2._1,x._2._1*x._2._1.t )).reduceByKey(_+_).map(x=> (x._1,breeze.linalg.pinv(x._2 + regMatrix))) 
	val sumruiyi = ratItemVec.map(x => (x._2._2._1,x._2._1 * x._2._2._2.toDouble )).reduceByKey(_+_) 
	val joinres = userbyItemMat.join(sumruiyi) 
	myuserMatrix = joinres.map(x=> (x._1,x._2._1 * x._2._2)).partitionBy(new HashPartitioner(10)) 

val ratUserVec = myuserMatrix.join(ratingByUser.value)
	val itembyUserMat = ratUserVec.map(x => (x._2._2._1,x._2._1*x._2._1.t )).reduceByKey(_+_).map(x=> (x._1,breeze.linalg.pinv(x._2 + regMatrix))) 
	val sumruixu = ratUserVec.map(x => (x._2._2._1,x._2._1 * x._2._2._2.toDouble )).reduceByKey(_+_) 
	val joinres1 = itembyUserMat.join(sumruixu) 
	myitemMatrix = joinres1.map(x=> (x._1,x._2._1 * x._2._2)).partitionBy(new HashPartitioner(10)) 
}

val user1 = myuserMatrix.filter(x=>(x._1.equals("1")))
val item1 = myitemMatrix.filter(x=>(x._1.equals("914")))
val usrarr1 = new DenseVector(user1.values.toArray)
val itmarr1 = new DenseVector(item1.values.toArray)
val finalrating1 = usrarr1 dot itmarr1

val user2 = myuserMatrix.filter(x=>(x._1.equals("1757")))
val item2 = myitemMatrix.filter(x=>(x._1.equals("1777")))
val usrarr2 = new DenseVector(user2.values.toArray)
val itmarr2 = new DenseVector(item2.values.toArray)
val finalrating2 = usrarr2 dot itmarr2

val user3 = myuserMatrix.filter(x=>(x._1.equals("1759")))
val item3 = myitemMatrix.filter(x=>(x._1.equals("231")))
val usrarr3 = new DenseVector(user3.values.toArray)
val itmarr3 = new DenseVector(item3.values.toArray)
val finalrating3 = usrarr3 dot itmarr3

print(usrarr1)
print(itmarr1)
print(finalrating1)
print(usrarr2)
print(itmarr2)
print(finalrating2)
print(usrarr3)
print(itmarr3)
print(finalrating3)
//1,1757 and 1759.
//914, 1777 and 231.
//======================================================Implement code to recalculate the ratings a user will give an item.====================

//Hint: This requires multiplying the latent vector of the user with the latent vector of the  item. Please take the input from the command line. and
// Provide the predicted rating for user 1 and item 914, user 1757 and item 1777, user 1759 and item 231.

//Your prediction code here


