def details(moviesFile:String, clusters: scala.collection.mutable.Map[Int, List[Array[Double]]]) = {		
var moviesData = sc.textFile("movies.dat");  
var temp = moviesData.map(line=>line.split("::")).map(line=>(line(0),line(1)+"----"+line(2))) 
for(k <- clusters.keys){  
println("  Cluster No "+k) 
var clusterRDD = sc.parallelize(clusters(k).take(5).map(line => line(0).toInt.toString)).collect.toSet; 
temp.filter({case(movieId, movieDetails)=>clusterRDD.contains(movieId)}).foreach(println);
}
}


def buildMap(k : Int) : collection.mutable.Map[Int, List[Array[Double]]] = {
var pointMap : collection.mutable.Map[Int, List[Array[Double]]] = collection.mutable.Map();
var temp_list : List[Array[Double]] = List();
for(i <- 0 until k){
pointMap += (i -> temp_list);
} 
return pointMap;
}

def dist(p1:Array[Double], p2: Array[Double]) : Double = {
   
	 return java.lang.Math.sqrt((p1 zip p2).map(x => java.lang.Math.pow(x._1-x._2, 2)).sum);
}

def pointsInCluster(data : List[Array[Double]], centroids : Array[Array[Double]]) : scala.collection.mutable.Map[Int, List[Array[Double]]] = {
var temp_list : List[Array[Double]] = List();
var pointMap = buildMap(centroids.length);
for(j <- 0 until data.length){
    var min = Double.PositiveInfinity;
	var index = 0;
    for(i <- 0 until centroids.length){
	val dis = dist(centroids(i), data(j));
	if(dis < min){
	   min = dis;
	   index = i;
	}
	}
	pointMap(index) = data(j) :: (pointMap getOrElseUpdate(index, temp_list));
}
return pointMap;
}

def findupdated_centroid(k:Int, clusters : scala.collection.mutable.Map[Int, List[Array[Double]]]) : Array[Array[Double]] ={
	var updated_centroid : Array[Array[Double]] = new Array[Array[Double]](k);
	for(i <- clusters.keys)
	{
		var temp = clusters(i);
		var ar = temp.transpose.map(_.sum).map(x => x/temp.size);
		updated_centroid(i) = ar.toArray;
	}	
	return updated_centroid;
}

def randomCentroids(data : List[Array[Double]], k : Int, len: Int) : Array[Array[Double]] = {
	val points = collection.mutable.HashSet[Int]();
	val randomGen = new scala.util.Random();
	while(points.size < k)
	{
		points += randomGen.nextInt(len);
	}
	val centroids = data.zipWithIndex.filter({case(_,index) => points.contains(index+1)}).map({case(x,y) => x});
	return centroids.toArray;
}

object p1_kmeans extends App
{
	override def main(args: Array[String])
	{
		val fileData = scala.io.Source.fromFile("itemusermat").getLines.map(_.split(" ").map(_.toDouble)).toList;
	
		val line_c = fileData.size;
		var centroids = randomCentroids(fileData, 10, line_c);
	
		var clusters = pointsInCluster(fileData, centroids)
		var updated_centroid = findupdated_centroid(10, clusters);
		var c = 0;
		while((centroids.deep != updated_centroid.deep) && (c < 25)){
		centroids = updated_centroid;
		clusters = pointsInCluster(fileData, centroids);
		updated_centroid = findupdated_centroid(10, clusters);
		c += 1;
	}
	details("movies.dat",clusters);
}
