from pyspark import SparkConf, SparkContext
import datetime
# import shutil, os

# conf = (SparkConf().setMaster('local').setAppName('wordCount1'))
# sc = SparkContext(conf = conf)

sc = SparkContext('spark://ec2-54-174-167-146.compute-1.amazonaws.com:7077', 'WordCount1')
inputPath = 'hdfs://ec2-54-174-167-146.compute-1.amazonaws.com:9000/user/zhoulin/input'
outputPath = 'hdfs://ec2-54-174-167-146.compute-1.amazonaws.com:9000/user/zhoulin/output-EX1'

# if (os.path.isdir(outputPath)):
# 	shutil.rmtree('outputPath')

start = datetime.datetime.now()
lines = sc.textFile(inputPath)

# def flatMap(line):
# 	return line.split(' ')

# def map(word):
# 	return (word, 1)

# def reduceByKey(a, b):
# 	return a+b

counts = lines.flatMap(lambda line: line.split(' '))\
.map(lambda word: (word, 1))\
.reduceByKey(lambda a, b: a + b)
# .map(list)\
# .map(lambda word: (word[0].encode('UTF8'), word[1]))

end = datetime.datetime.now()
elapsed_time = end - start

counts.saveAsTextFile(outputPath)
print ('EX1 elapsed_time is:' + str(elapsed_time) + 's')
