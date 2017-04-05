from pyspark import SparkConf, SparkContext
import datetime

# conf = (SparkConf().setMaster('local').setAppName('wordCount1'))
# sc = SparkContext(conf = conf)

sc = SparkContext('spark://ec2-54-174-167-146.compute-1.amazonaws.com:7077', 'WordCount2')
inputPath = 'hdfs://ec2-54-174-167-146.compute-1.amazonaws.com:9000/user/zhoulin/input'
outputPath = 'hdfs://ec2-54-174-167-146.compute-1.amazonaws.com:9000/user/zhoulin/output-EX2'

start = datetime.datetime.now()
lines = sc.textFile(inputPath)

# def split_to_double(words):
# 	ls = []
# 	for i in range(0, len(words)):
# 		for j in range(len(words[i])-1):
# 			print (type(words[i][j]))
# 			ls.append(words[i][j].encode('UTF8') + ' ' + words[i][j + 1].encode('UTF8'))
# 	return ls

def split_to_pair(words):
	ls = []
	for i in range(len(words)-1):
		pair = words[i] + ' ' + words[i+1]
		ls.append(pair)
	return ls

counts = sc.parallelize(split_to_pair(lines.flatMap(lambda line: line.split()).collect()))\
.map(lambda pair: (pair, 1))\
.reduceByKey(lambda a, b: a + b)

end = datetime.datetime.now()
elapsed_time = end - start

counts.saveAsTextFile(outputPath)
print ('EX2 elapsed_time is:' + str(elapsed_time) + 's')

# temp = lines.flatMap(lambda line: line.split()).collect()
# ls = []
# # for i in range(len(temp)):
# # 	# list to str
# # 	words = ' '.join(temp[i]).split()
# # 	for j in range(len(words) - 1):
# # 		# pair double words
# # 		pairs = words[j] + ' ' + words[j+1]
# # 		ls.append(pairs)

# for i in range(len(temp)-1):
# 	pair = temp[i] + ' ' + temp[i+1]
# 	print(pair)
# 	ls.append(pair)




# for i in range(len(temp)):
# 	words = ' '.join(temp[i]).split()
# 	pairs = zip(words, words[1:])
# 	ls.append(pairs)

	# print(pairs)
	# print(type(pairs))
	# print(type(pairs[0]))

# temp = lines.map(lambda line:line.split())
# print (type(temp))

# counts = sc.parallelize(split_to_double(lines.map(lambda line: line.split()).collect())).reduceByKey(lambda a, b: a + b)

# counts = sc.parallelize(ls).map(lambda pair: (pair, 1)).reduceByKey(lambda a, b: a + b)


