[TOC]

Spark是一个集群计算框架，它使用内存中的原语使程序运行速度比Hadoop Map Reduce应用程序快一百倍。 Spark应用程序由一个驱动程序组成，该程序控制跨集群的并行操作的执行。 Spark提供的主要编程抽象称为弹性分布式数据集（RDD）。 RDD是跨群集节点分区的元素的集合，可以并行操作。

Spark被创建为在许多平台上运行并以多种语言开发。 目前，Spark可以在Hadoop 1.0，Hadoop2.0，Apache Mesos或独立的Spark集群上运行。 Spark本身也支持Scala，Java，Python和R.除了这些功能外，还可以从命令行shell以交互方式使用Spark。

本章以示例Spark脚本开头。 然后介绍PySpark，并通过示例详细描述RDD。 本章以用Python编写的示例Spark程序结束。

# WordCount in PySpark

例4-1中的代码在PySpark中实现了WordCount算法。 它假定数据文件input.txt在/user/hduser/input下加载到HDFS中，输出将放在/user/hduser/output下的HDFS中。

**Example 4-1. python/Spark/word_count.py**

```
from pyspark import SparkContext

def main():   
    sc = SparkContext(appName='SparkWordCount')   
    input_file = sc.textFile('/user/hduser/input/input.txt')
    counts = input_file.flatMap(lambda line: line.split()) \ 
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile('/user/hduser/output')   
    
    sc.stop()

if __name__ == '__main__':   
    main()
```

要执行Spark应用程序，请将文件名传递给spark-submit脚本：

```
$ spark-submit --master local word_count.py
```

当作业运行时，很多文本将被打印到控制台。 word_count.py Spark脚本的结果显示在例4-2中，可以在/user/hduser/output/part-00000下的HDFS中找到。

**Example 4-2. /user/hduser/output/part-00000**

```
(u'be', 2)
(u'jumped', 1)
(u'over', 1)
(u'candlestick', 1)
(u'nimble', 1)
(u'jack', 3)
(u'quick', 1)
(u'the', 1)
```
## WordCount Described
本节介绍在word_count.py Spark脚本中应用的转换。

第一个语句创建一个SparkContext对象。 该对象告诉Spark如何以及在何处访问集群：

```
sc = SparkContext(appName='SparkWordCount')
```
第二个语句使用SparkContext从HDFS加载文件并将其存储在变量input_file中：

```
input_file = sc.textFile('/user/hduser/input/input.txt')
```
第三个语句对输入数据执行多次转换。 Spark自动并行化这些转换以跨多台机器运行：

```
counts = input_file.flatMap(lambda line: line.split()) \
                    .map(lambda word: (word, 1)) \ 
                    .reduceByKey(lambda a, b: a + b)
```
第四个语句将结果存储到HDFS：

```
counts.saveAsTextFile('/user/hduser/output')
```
第五个语句关闭SparkContext：

```
sc.stop()
```

# PySpark

PySpark是Spark的Python API。 PySpark允许从交互式shell或Python程序创建Spark应用程序。

在Spark中执行任何代码之前，应用程序必须创建一个SparkContext对象。SparkContext对象告诉Spark如何以及在何处访问集群。master属性是一个集群URL，用于确定Spark应用程序的运行位置。 master的最常见值是：

```
local
    Run Spark with one worker thread.
local[n]
    Run Spark with n worker threads.
spark://HOST:PORT
    Connect to a Spark standalone cluster.
mesos://HOST:PORT
    Connect to a Mesos cluster.
```

## Interactive Shell

在Spark shell中，将在shell启动时创建SparkContext。 SparkContext保存在变量sc中。 在启动shell时，可以使用--master参数设置交互式shell的主服务器。 要启动交互式shell，请运行pyspark命令：

```
$ pyspark --master local[4]
...
Welcome to
      ____              __
     / __/__  ___ _____/ /__    
    _\ \/ _ \/ _ `/ __/  '_/   
   /__ / .__/\_,_/_/ /_/\_\   version 1.5.0      
      /_/
Using Python version 2.7.10 (default, Jul 13 2015 12:05:58)
SparkContext  available  as  sc,  HiveContext  available  as  sqlCon-text.
>>>
```
有关选项的完整列表，请运行pyspark --help。

## Self-Contained Applications

在使用任何Spark方法之前，自包含应用程序必须首先创建SparkContext对象。 调用SparkContext()方法时可以设置master：

```
sc = SparkContext(master='local[4]')
```
要执行自包含的应用程序，必须将它们提交给spark-submit脚本。 spark-submit脚本包含许多选项; 要查看完整列表，请从命令行运行spark-submit --help：

```
$ spark-submit --master local spark_app.py
```
# Resilient Distributed Datasets (RDDs)
弹性分布式数据集（RDD）是Spark中的基本编程抽象。 RDD是跨机器分区的不可变数据集合，可以并行地对元素执行操作。 RDD可以通过多种方式构建：通过并行化现有Python集合，引用外部存储系统（如HDFS）中的文件，或者将转换应用于现有RDD。

## Creating RDDs from Collections
可以通过调用SparkcContext.parallelize()方法从Python集合创建RDD。 复制集合的元素以形成可以并行操作的分布式数据集。 以下示例从Python列表创建并行化集合：

```
>>> data = [1, 2, 3, 4, 5]
>>> rdd = sc.parallelize(data)
>>> rdd.glom().collect()
...
[[1, 2, 3, 4, 5]]
```
RDD.glom()方法返回每个分区中所有元素的列表，RDD.collect()方法将所有元素都带到驱动程序节点。 结果[[1,2,3,4,5]]是列表中的原始集合。

要指定应使用RDD创建的分区数，可以将第二个参数传递给parallelize()方法。 以下示例在上一个示例中从相同的Python集合创建RDD，但这次创建了四个分区：

```
>>> rdd = sc.parallelize(data, 4)
>>> rdd.glom().collect()
...
[[1], [2], [3], [4, 5]]
```
使用glom()和collect()方法，在此示例中创建的RDD包含四个内部列表：[1]，[2]，[3]和[4,5]。 内部列表的数量表示RDD中的分区数。

## Creating RDDs from External Sources
也可以使用SparkContext.textFile()方法从文件创建RDD。 Spark可以读取驻留在本地文件系统上的文件，Hadoop，Amazon S3等支持的任何存储源。 Spark支持文本文件，SequenceFiles，任何其他Hadoop InputFormat，目录，压缩文件和通配符，例如my/directory/* .txt。 以下示例从位于本地文件系统上的文件创建分布式数据集：

```
>>> distFile = sc.textFile('data.txt')
>>> distFile.glom().collect()
...
[[u'jack be nimble', u'jack be quick', u'jack jumped over the candlestick']]
```
和以前一样，glom()和collect()方法允许RDD显示在其分区中。 此结果表明distFile只有一个分区。

与parallelize()方法类似，textFile()方法接受第二个参数，该参数指定要创建的分区数。 以下示例使用输入文件中的三个分区创建RDD：

```
>>> distFile = sc.textFile('data.txt', 3)
>>> distFile.glom().collect()
...
[[u'jack  be  nimble',  u'jack  be  quick'],  [u'jack  jumped  over the candlestick'], []]
```

## RDD Operations
RDD支持两种类型的操作：转换和操作。 转换从现有数据集创建新数据集，并且操作在数据集上运行计算并将结果返回给驱动程序。

转换是懒惰的：也就是说，它们的结果不是立即计算的。 相反，Spark会记住应用于基础数据集的所有转换。 当动作需要将结果返回到驱动程序时，计算转换。 这允许Spark有效地运行，并且仅在动作之前传输转换的结果。

默认情况下，每次对其执行操作时都可以重新计算转换。 这允许Spark有效地利用内存，但如果不断处理相同的转换，它可以利用更多的处理资源。 为了确保只计算一次转换，可以使用RDD.cache()方法将生成的RDD保存在内存中。

### RDD  Workflow
使用RDD的一般工作流程如下：
1. 从数据源创建RDD。
2. 将转换应用于RDD。
3. 将操作应用于RDD。  

以下示例使用此workflow计算文件中的字符数：

```
>>> lines = sc.textFile('data.txt')
>>> line_lengths = lines.map(lambda x: len(x))
>>> document_length = line_lengths.reduce(lambda x,y: x+y)
>>> print document_length
59
```
第一个语句从外部文件data.txt创建RDD。 此时不加载此文件; 变量行只是指向外部源的指针。 第二个语句通过使用map()函数计算每行中的字符数，对基本RDD执行转换。 由于变换的懒惰，不立即计算变量line_lengths。 最后，调用reduce()方法，这是一个Action。 此时，Spark将计算划分为在不同计算机上运行的任务。 每台机器都运行map并reduce其本地数据，仅将结果返回给driver程序。

如果应用程序再次使用line_lengths，则最好保留映射转换的结果，以确保不会重新计算映射。 以下行将在第一次计算后将line_lengths保存到内存中：

```
>>> line_lengths.persist()
```
### Python Lambda Functions
Spark的许多转换和操作都需要从driver程序传递函数对象以在集群上运行。 定义和传递函数的最简单方法是使用Python lambda函数。

Lambda函数是在运行时创建的匿名函数（即，它们没有名称）。 它们可以在需要函数对象的任何地方使用，并在语法上限制为单个表达式。 以下示例显示了一个lambda函数，该函数返回其两个参数的总和：

```
lambda a, b: a + b
```
Lambda由关键字lambda定义，后跟一个逗号分隔的参数列表。 冒号将函数声明与函数表达式分开。 函数表达式是一个单独的表达式，它为所提供的参数生成结果。

在前面的Spark示例中，map()函数使用以下lambda函数：

```
lambda x: len(x)
```
这个lambda有一个参数并返回参数的长度。

### Transformations
转换从现有数据集创建新数据集。 延迟的转换评估允许Spark记住应用于基础RDD的转换集。 这使Spark能够优化所需的计算。

本节介绍Spark最常见的一些转换。 有关转换的完整列表，请参阅Spark的Python RDD API文档。

**map.** map(func)函数通过将函数func应用于源的每个元素来返回新的RDD。 以下示例将源RDD的每个元素乘以2：

```
>>> data = [1, 2, 3, 4, 5, 6]
>>> rdd = sc.parallelize(data)
>>> map_result = rdd.map(lambda x: x * 2)
>>> map_result.collect()
[2, 4, 6, 8, 10, 12]
```
**filter.** filter(func)函数返回一个新的RDD，它只包含所提供函数返回的源元素为true。 以下示例仅返回源RDD中的偶数：

```
>>> data = [1, 2, 3, 4, 5, 6]
>>> filter_result = rdd.filter(lambda x: x % 2 == 0)
>>> filter_result.collect()
[2, 4, 6]
```
**distinct.** distinct()方法返回一个新的RDD，它只包含源RDD中的不同元素。 以下示例返回列表中的唯一元素：

```
>>> data = [1, 2, 3, 2, 4, 1]
>>> rdd = sc.parallelize(data)
>>> distinct_result = rdd.distinct()
>>> distinct_result.collect()
[4, 1, 2, 3]
```
**flatMap.** flatMap(func)函数类似于map()函数，除了它返回结果的展平版本。 为了进行比较，以下示例从源RDD及其正方形返回原始元素。 使用map()函数的示例将对返回为列表中的列表：

```
>>> data = [1, 2, 3, 4]>>> rdd = sc.parallelize(data)
>>> map = rdd.map(lambda x: [x, pow(x,2)])
>>> map.collect()
[[1, 1], [2, 4], [3, 9], [4, 16]]
```
虽然flatMap()函数连接结果，但返回一个列表：

```
>>> rdd = sc.parallelize()
>>> flat_map = rdd.flatMap(lambda x: [x, pow(x,2)])
>>> flat_map.collect()
[1, 1, 2, 4, 3, 9, 4, 16]
```
### Actions
操作会导致Spark计算转换。 在集群上计算转换后，结果将返回给driver程序。

以下部分描述了Spark最常见的一些操作。 有关操作的完整列表，请参阅Spark的Python RDD API文档。

**reduce.** reduce()方法使用一个函数聚合RDD中的元素，该函数接受两个参数并返回一个参数。 reduce方法中使用的函数是可交换和关联的，确保它可以并行正确计算。 以下示例返回RDD中所有元素的乘积：

```
>>> data = [1, 2, 3]
>>> rdd = sc.parallelize(data)
>>> rdd.reduce(lambda a, b: a * b)
6
```
**take.** take(n)方法返回一个包含RDD前n个元素的数组。 以下示例返回RDD的前两个元素：

```
>>> data = [1, 2, 3]
>>> rdd = sc.parallelize(data)
>>> rdd.take(2)
[1, 2]
```
**collect.** collect()方法将RDD的所有元素作为数组返回。 以下示例返回RDD中的所有元素：

```
>>> data = [1, 2, 3, 4, 5]
>>> rdd = sc.parallelize(data)
>>> rdd.collect()
[1, 2, 3, 4, 5]
```
值得注意的是，在大型数据集上调用collect()可能会导致driver程序内存不足。 要检查大型RDD，可以使用take()和collect()方法检查大型RDD的前n个元素。 以下示例将RDD的前100个元素返回给驱动程序：

```
>>> rdd.take(100).collect()
```
**takeOrdered.** takeOrdered(n，key = func)方法以其自然顺序返回RDD的前n个元素，或者由函数func指定。 以下示例按降序返回RDD的前四个元素：

```
>>> data = [6,1,5,2,4,3]
>>> rdd = sc.parallelize(data)
>>> rdd.takeOrdered(4, lambda s: -s)
[6, 5, 4, 3]
```

# Text Search with PySpark
文本搜索程序搜索与给定字符串匹配的电影标题（例4-3）。 电影数据来自groupLens数据集; 应用程序希望将其存储在/user/hduser/input/movies下的HDFS中。

**Example 4-3. python/Spark/text_search.py**

```
from pyspark import SparkContext
import re
import sys

def main():   
    # Insure a search term was supplied at the command line   
    if len(sys.argv) != 2:       
        sys.stderr.write('Usage:  {}  <search_term>'.format(sys.argv[0]))      
        sys.exit()   
    
    # Create the SparkContext   
    sc = SparkContext(appName='SparkWordCount')   
    
    # Broadcast the requested term   
    requested_movie = sc.broadcast(sys.argv[1])   
    
    # Load the input file   
    source_file = sc.textFile('/user/hduser/input/movies')   
    
    # Get the movie title from the second fields   
    titles = source_file.map(lambda line: line.split('|')[1])   
    
    # Create a map of the normalized title to the raw title
    normalized_title  =  titles.map(lambda  title:  (re.sub(r'\s*\(\d{4}\)','', title).lower(), title))      
    
    # Find all movies matching the requested_movie    
    matches  =  normalized_title.filter(lambda  x:  requested_movie.value in x[0])   
    
    # Collect all the matching titles    
    matching_titles  =  matches.map(lambda  x:  x[1]).distinct().collect()   
    
    # Display the result   
    print '{} Matching titles found:'.format(len(matching_titles))
    for title in matching_titles:      
        print title   
    
    sc.stop()

if __name__ == '__main__':   
    main()
```

可以通过向spark-submit脚本传递程序名称text_search.py以及要搜索的术语来执行Spark应用程序。 可以在此处看到应用程序的示例运行：

```
$ spark-submit text_search.py gold
...
6 Matching titles found:
GoldenEye (1995)
On Golden Pond (1981)
Ulee's Gold (1997)
City Slickers II: The Legend of Curly's Gold (1994)
Golden Earrings (1947)
Gold Diggers: The Secret of Bear Mountain (1995)
...
```
由于计算转换可能是一项代价高昂的操作，因此Spark可以将normalized_titles的结果缓存到内存中，以加快将来的搜索速度。 从上面的示例中，要将normalized_titles加载到内存中，请使用cache()方法：

```
normalized_title.cache()
```
# (章节总结) Chapter Summary
本章介绍了Spark和PySpark。 它描述了Spark的主要编程抽象，RDD，以及许多数据集转换的例子。 本章还包含一个Spark应用程序，它返回与给定字符串匹配的电影标题。


