from pyspark import SparkConf, SparkContext
def wordlength():
    conf = SparkConf().setAppName("WordCount").setMaster("local")
    Context = SparkContext(conf=conf)
    textFile = Context.textFile("dataset/hamlet.txt")
    wordLength = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (len(word), 1)).reduceByKey(lambda a, b: a + b)
    wordlength_dict = wordLength.collectAsMap()
    Context.stop()
    return wordlength_dict
def main():
    print( wordlength())
    

if __name__ == "__main__":
    main()
