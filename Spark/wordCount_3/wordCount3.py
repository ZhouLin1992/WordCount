from pyspark import SparkConf, SparkContext
# from pydoop.hdfs as hdfs
import datetime

sc = SparkContext('spark://ec2-54-174-167-146.compute-1.amazonaws.com:7077', 'WordCount3')
# sc_cache = SparkContext()
cachePath = 'hdfs://ec2-54-174-167-146.compute-1.amazonaws.com:9000/user/zhoulin/cache/'
inputPath = 'hdfs://ec2-54-174-167-146.compute-1.amazonaws.com:9000/user/zhoulin/input/'
outputPath = 'hdfs://ec2-54-174-167-146.compute-1.amazonaws.com:9000/user/zhoulin/output-EX3/'

# read cache words
def cache_lst(words):
	lst = []
	for i in range(len(words)):
		lst.append(words[i])
	return lst

# compare cache words with input words
def comp(i, c):
	if i == c:
		return c

start = datetime.datetime.now()

cache_ln = sc.textFile(cachePath).flatMap(lambda line: line.split()).collect()
input_ln = sc.textFile(inputPath).flatMap(lambda line: line.split()).collect()
cache_list = cache_lst(cache_ln)

ls = []
for j in range(len(cache_list)):
	w = cache_list[j]
	for k in range(len(input_ln)):
		if (comp(input_ln[k], w)):
			ls.append(w)

counts = sc.parallelize(ls).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# if exist(outputPath):
#     # Remove old results from HDFS
#     try:
#         hdfs.rmr(outputPath)
#     except:
#         pass

end = datetime.datetime.now()
elapsed_time = end - start

counts.saveAsTextFile(outputPath)
print ('EX1 elapsed_time is:' + str(elapsed_time) + 's')
