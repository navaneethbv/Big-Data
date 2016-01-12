import sys
import getopt

from pyspark import SparkContext,SparkConf 

search = "" 

def f((x,y)): print(x+" "+y)
def filt((x,y)):
        global search  
        return search in y

def main(): 
        sc = SparkContext()
        text = sc.textFile("/yelpdatafall/business/business.csv")
        words = text.map(lambda x : (x.split("^")[0],x.split("^")[1]))
        #print 'Argument List:', str(sys.argv[1])       
        global search
        search= sys.argv[1]
        loc = words.filter(filt).foreach(f)
        return 0

if __name__ == "__main__":
        main()
