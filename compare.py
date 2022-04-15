import sys
from wordcountspark.main import wordcount
from wordlengthspark.main import wordlength
from moviecountspark.spark import moviecount

# read udf output and store in a dict
def readfiletodict(location):
    file = open(location, "r",encoding="utf8")
    file = file.readlines()
    file = dict(map(lambda line: line.split(" "), file))
    return file

class Compare():
    def __init__(self):
        self.wordcount_result = wordcount()
        self.wordlength_result = wordlength()
        self.moviecount_result = moviecount()
        self.wordcount_output = readfiletodict("output/hamlet_wordcount_out.txt")
        self.wordlength_output = readfiletodict("output/hamlet_wordlength_out.txt")
        self.movie_output = readfiletodict("output/movie_out.txt")

# compare 2 dict iteratively
# compare 2 dict iteratively
    def compare(self, dict1, dict2):
        ##if len(dict1) != len(dict2):
            ##return False
        for key, value in dict2.items():
            key = str(key)
            a = dict1.get(key,None)
            if key not in dict1.keys():
                return False
            if int(a) != value:
                return False
        else:
            return True



    def print_top5(self, Dict):
        count = 0
        for key, value in Dict.items():
            print(key, ":", int(value))
            count += 1
            if count == 5:
                break

    def compare_result(self, dict1, dict2, Category):
        print("-------------------------------------------------------------------------")
        print("Comparing our {} and spark result...".format(Category))
        print("Our functions output == Spark output?", self.compare(dict1, dict2))
        print("The top five key-value pairs of both...")
        print("Our function:")
        self.print_top5(dict1)
        print("\nSpark:")
        self.print_top5(dict2)
        print()



def main():
    compare = Compare()
    compare.compare_result(compare.wordcount_output, compare.wordcount_result, "WordCount")
    compare.compare_result(compare.wordlength_output, compare.wordlength_result, "WordLength")
    compare.compare_result(compare.movie_output, compare.moviecount_result, "MovieCount")

if __name__ == "__main__":
    sys.exit(main())