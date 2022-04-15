from pyspark import SparkConf, SparkContext
def wordcount():
    conf = SparkConf().setAppName("WordCount").setMaster("local")
    Context = SparkContext(conf=conf)
    textFile = Context.textFile("dataset/hamlet.txt")
    wordCount = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    wordCount_dict = wordCount.collectAsMap()
    del wordCount_dict[""]
    Context.stop()
    return wordCount_dict
