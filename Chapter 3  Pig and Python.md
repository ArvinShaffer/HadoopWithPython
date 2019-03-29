[TOC]

Pig由两个主要部分组成：一个名为Pig Latin的高级数据流语言，以及一个解析，优化和执行Pig Latin脚本的引擎，作为在Hadoop集群上运行的一系列MapReduce作业。 与Java MapReduce相比，Pig更易于编写，理解和维护，因为它是一种数据转换语言，允许将数据处理描述为一系列转换。 通过使用用户定义函数（UDF），Pig也具有高度可扩展性，允许自定义处理以多种语言编写，例如Python。

Pig应用程序的一个示例是提取，转换，加载（ETL）过程，该过程描述应用程序如何从数据源中提取数据，转换数据以进行查询和分析，并将结果加载到目标数据存储中。 一旦Pig加载数据，它就可以执行投影，迭代和其他转换。 UDF允许在转换阶段应用更复杂的算法。 在Pig处理数据之后，它可以存储回HDFS。

本章以Pig脚本示例开头。 然后引入Pig和Pig Latin并用实例详细描述。 本章最后解释了如何通过使用Python扩展Pig的核心功能。

# WordCount in Pig

例3-1在Pig中实现了WordCount算法。 它假定在/user/hduser/input下的HDFS中加载了一个数据文件input.txt，输出将放在/user/hduser/output下的HDFS中。

**Example 3-1. pig/wordcount.pig**

```
%default INPUT '/user/hduser/input/input.txt';
%default OUTPUT '/user/hduser/output';

-- Load the data from the file system into the relation records
records = LOAD '$INPUT';

-- Split each line of text and eliminate nesting
terms = FOREACH records GENERATE FLATTEN(TOKENIZE((chararray) $0)) AS word;

-- Group similar terms
grouped_terms = GROUP terms BY word;

-- Count the number of tuples in each group
word_counts = FOREACH grouped_terms GENERATE COUNT(terms), group;

-- Store the result
STORE word_counts INTO '$OUTPUT';
```
要执行Pig脚本，只需从命令行调用Pig并将其传递给要运行的脚本的名称：

```
$ pig wordcount.pig
```
当作业运行时，很多文本将被打印到控制台。 作业完成后，将显示类似于下面的成功消息：

```
2015-09-26  14:15:10,030  [main]  INFO   org.apache.pig.back-end.hadoop.executionengine.mapReduceLayer.MapReduceLauncher  - Success!
2015-09-26 14:15:10,049 [main] INFO  org.apache.pig.Main - Pig script completed in 18 seconds and 514 milliseconds (18514 ms)
```
wordcount.pig脚本的结果显示在例3-2中，可以在/user/hduser/output/pig_wordcount/part-r-00000下的HDFS中找到。

**Example 3-2. /user/hduser/output/pig_wordcount/part-r-00000**

```
2    be
1    the
3    jack
1    over
1    quick
1    jumped
1    nimble
1    candlestick
```

## WordCount in Detail

本节介绍wordcount.pig脚本中的每个Pig Latin语句。

第一个语句从文件系统加载数据并将其存储在关系记录中：

```
records = LOAD '/user/hduser/input/input.txt';
```
第二个语句使用TOKENIZE函数拆分每行文本，并使用FLATTEN运算符消除嵌套：

```
terms  =  FOREACH  records  GENERATE  FLATTEN(TOKENIZE((chararray)$0)) AS word;
```
第三个语句使用GROUP运算符对具有相同字段的元组进行分组：

```
grouped_terms = GROUP terms BY word;
```
第四个语句迭代每个包中的所有术语，并使用COUNT函数返回总和：

```
word_counts  =  FOREACH  grouped_terms  GENERATE  COUNT(terms), group;
```
第五个也是最后一个语句将结果存储在HDFS中：

```
STORE word_counts INTO '/user/hduser/output/pig_wordcount'
```


# Running Pig

Pig包含多种模式，可以指定这些模式来配置Pig脚本和Pig语句的执行方式。

## Execution Modes

Pig有两种执行模式：local和MapReduce。

在本地模式下运行Pig只需要一台机器。 Pig将在本地主机上运行并访问本地文件系统。 要以本地模式运行Pig，请使用-x local标志：

```
$ pig -x local ...
```
在MapReduce模式下运行Pig需要访问Hadoop集群。 MapReduce模式在集群上执行Pig语句和作业并访问HDFS。 要在MapReduce模式下运行Pig，只需从命令行调用Pig或使用-x mapreduce标志：

```
$ pig ...
or
$ pig -x mapreduce ...
```
## Interactive Mode

Pig可以在Grunt shell中以交互方式运行。 要调用Grunt shell，只需从命令行调用Pig并指定所需的执行模式。 以下示例以本地模式启动Grunt shell：

```
pig -x local
...
grunt>
```
初始化Grunt shell后，可以以交互方式输入和执行Pig Latin语句。 交互式运行pig是学习pig的好方法。

以下示例读取/etc/passwd并显示Grunt shell中的用户名：

```
grunt> A = LOAD '/etc/passwd' using PigStorage(':');
grunt> B = FOREACH A GENERATE $0 as username;
grunt> DUMP B;
```
## Batch Mode

批处理模式允许Pig以本地或MapReduce模式执行Pig脚本。


例3-3中的Pig Latin语句读取名为passwd的文件，并使用STORE运算符将结果存储在名为user_id.out的目录中。 在执行此脚本之前，如果Pig将以本地模式运行，请确保将/etc/passwd复制到当前工作目录，如果Pig将在MapReduce模式下执行，则确保将/etc/passwd复制到HDFS。

**Example 3-3. pig/user_id.pig**
```
A = LOAD 'passwd' using PigStorage(':');
B = FOREACH A GENERATE $0 as username;
STORE B INTO 'user_id.out';
```
使用以下命令在本地计算机上执行user_id.pig脚本：

```
$ pig -x local user_id.pig
```

# Pig Latin 
本节描述了Pig Latin语言的基本概念，允许那些不熟悉该语言的人理解和编写基本的Pig脚本。 有关该语言的更全面概述，请访问Pig在线文档。

本节中的所有示例都以制表符分隔的resources/students（示例3-4）加载和处理数据。

**Example 3-4. resources/students**

```
john    21    3.89
sally    19    2.56
alice    22    3.76
doug    19    1.98
susan    26    3.25
```
## Statements

语句是用于处理Pig中数据的基本结构。 每个语句都是一个运算符，它将关系作为输入，对该关系执行转换，并生成关系作为输出。 语句可以跨越多行，但所有语句必须以分号（;）结尾。

每个Pig脚本的一般形式如下：

- 1.一个LOAD语句，用于从文件系统中读取数据
- 2.一个或多个转换数据的语句
- 3.分别用于查看或存储结果的DUMP或STORE语句

## Loading Data
LOAD运算符用于将数据从系统加载到Pig。 LOAD运算符的格式如下：

```
LOAD 'data' [USING function] [AS schema];
```
其中'data'是要加载的文件或目录的名称，用引号括起来。 如果未指定目录名，则加载目录中的所有文件。

USING关键字是可选的，用于指定解析传入数据的函数。 如果省略USING关键字，则使用默认加载函数PigStorage。 默认分隔符是制表符（'\ t'）。

AS关键字允许为正在加载的数据定义模式。 模式允许为各个字段声明名称和数据类型。 以下示例为从input.txt文件加载的数据定义模式。 如果未定义架构，则不会命名字段，并且默认键入bytearray。

```
A = LOAD 'students' AS (name:chararray, age:int);

DUMP A;
(john,21,3.89)
(sally,19,2.56)
(alice,22,3.76)
(doug,19,1.98)
(susan,26,3.25)
```

## Transforming Data

Pig包含许多能够实现复杂数据转换的运算符。 最常见的运算符是FILTER，FOREACH和GROUP。

### FILTER
FILTER运算符处理元组或数据行。 它根据条件从关系中选择元组。

以下示例使用包含学生数据的关系A：

```
A = LOAD 'students' AS (name:chararray, age:int, gpa:float);

DUMP A;
(john,21,3.89)
(sally,19,2.56)
(alice,22,3.76)
(doug,19,1.98)
(susan,26,3.25)
```
以下示例过滤掉20岁以下的所有学生，并将结果存储在关系R中：

```
R = FILTER A BY age >= 20;

DUMP R;
(john,21,3.89)
(alice,22,3.76)
(susan,26,3.25)
```
条件语句可以使用AND，OR和NOT运算符来创建更复杂的FILTER语句。 以下示例筛选出年龄小于20或GPA小于或等于3.5的学生，并将结果存储在关系R中：

```
R = FILTER A BY (age >= 20) AND (gpa > 3.5);

DUMP R;
(john,21,3.89)
(alice,22,3.76)
```
### FOREACH
当FILTER运算符处理数据行时，FOREACH运算符处理数据列，类似于SQL中的SELECT语句。

以下示例使用星号（*）将关系A中的所有字段投影到关系X：

```
R = FOREACH A GENERATE *;

DUMP R;
(john,21,3.89)
(sally,19,2.56)
(alice,22,3.76)
(doug,19,1.98)
(susan,26,3.25)
```
以下示例使用字段名称将关系A中的age和gpa列投影到关系X：

```
R = FOREACH A GENERATE age, gpa;

DUMP R;
(21,3.89)
(19,2.56)
(22,3.76)
(19,1.98)
(26,3.25)
```
### GROUP

GROUP运算符将具有相同组密钥的元组组合成一个或多个关系。

以下示例按年龄对学生数据进行分组，并将结果存储到关系B中：

```
B = GROUP A BY age;

DUMP B;
(19,{(doug,19,1.98),(sally,19,2.56)})
(21,{(john,21,3.89)})
(22,{(alice,22,3.76)})
(26,{(susan,26,3.25)})
```
GROUP操作的结果是每个组有一个元组的关系。 该元组有两个字段：第一个字段命名为group，属于分组键的类型; 第二个字段是一个包含原始关系名称的包。 为了阐明关系B的结构，可以使用DESCRIBE和ILLUSTRATE操作：


```
DESCRIBE B;
B: {group: int,A: {(name: chararray,age: int,gpa: float)}}
ILLUSTRATE B;
------------------------------------------------------------
| B  | group:int  | A:bag{:tuple(name:chararray,            |
                    age:int,gpa:float)}                     |
-------------------------------------------------------------
|    | 19         | {(sally, 19, 2.56), (doug, 19, 1.98)}   |
-------------------------------------------------------------
```
使用FOREACH运算符，可以通过名称组和A来引用先前关系B中的字段：

```
C = FOREACH B GENERATE group, A.name;

DUMP C;
(19,{(doug),(sally)})
(21,{(john)})
(22,{(alice)})
(26,{(susan)})
```
## Storing Data
STORE运算符用于执行以前的Pig语句并将结果存储在文件系统中。 STORE运算符的格式如下：

```
STORE alias INTO 'directory' [USING function];
```
别名是要存储的关系的名称，'directory'是存储目录的名称，用引号括起来。 如果该目录已存在，则STORE操作将失败。 输出文件将命名为part-nnnnn并写入指定的目录。

USING关键字是可选的，用于指定存储数据的函数。 如果省略USING关键字，则使用默认存储功能PigStorage。 以下示例指定PigStorage函数以存储具有竖线分隔字段的文件：

```
A = LOAD 'students' AS (name:chararray, age:int, gpa:float);

DUMP A;
(john,21,3.89)
(sally,19,2.56)
(alice,22,3.76)
(doug,19,1.98)
(susan,26,3.25)

STORE A INTO 'output' USING PigStorage('|');

CAT output;
john|21|3.89
sally|19|2.56
alice|22|3.76
doug|19|1.98
susan|26|3.25
```

提供的Pig Latin语句是很好的通用计算结构，但不能表达复杂的算法。 下一节将介绍如何使用Python扩展Pig的功能。

# Extending Pig with Python
Pig通过用户定义函数（UDF）为自定义处理提供了广泛的支持。 Pig目前支持六种语言的UDF：Java，Jython，Python，JavaScript，Ruby和Groovy。

当Pig执行时，它会自动检测UDF的使用情况。 为了运行Python UDF，Pig调用Python命令行并将数据流入和流出。

## Registering a UDF

在可以在Pig脚本中使用Python UDF之前，必须对其进行注册，以便Pig知道调用UDF时的位置。 要注册Python UDF文件，请使用Pig的REGISTER语句：

```
REGISTER 'udfs/myudf.py' USING streaming_python AS my_udf;
```
注册UDF后，可以从Pig脚本中调用它：

```
relation = FOREACH data GENERATE my_udf.function(field);
```
在此示例中，引用为my_udf的UDF包含一个名为function的函数。

## A Simple Python UDF
位于pig/udfs/my_first_udf.py中的简单Python UDF，在每次调用时返回整数值1，如例3-5所示。

**Example 3-5. pig/udfs/my_first_udf.py**

```
from pig_util import outputSchema

@outputSchema('value:int')
def return_one():   
    """   
    Return the integer value 1   
    """   
    return 1
```
在这个Python脚本中需要注意的一些重要事项是第一行的from语句和第三行的输出装饰器@outputSchema装饰器。 这些行使Python UDF能够为从UDF返回的数据定义别名和数据类型。

示例3-6中的Pig脚本注册Python UDF并在FOREACH语句中调用return_one()函数。

**Example 3-6. pig/simple_udf.pig**

```
REGISTER 'udfs/my_first_udf.py' USING streaming_python AS pyudfs;

A = LOAD '../resources/input.txt';
B = FOREACH A GENERATE pyudfs.return_one();
DUMP B;
```
执行Pig脚本时，它将为输入文件中的每一行生成一个整数值1。 使用以下命令执行脚本（也显示示例输出）：

```
$ pig -x local simple_udf.pig
...
(1)
(1)
(1)
```
## String Manipulation

Python UDF是扩展Pig功能的简便方法，也是转换和处理数据的简便方法。

例3-7中的Python UDF包含两个函数：reverse()和num_chars()。 reverse()函数接受chararray并以相反的顺序返回chararray。 num_chars()函数接受chararray并返回chararray中的字符数。

**Example 3-7. pig/udfs/string_funcs.py**

```
from pig_util import outputSchema

@outputSchema('word:chararray')
def reverse(word):   
    """   
    Return the reverse text of the provided word   
    """   
    return word[::-1]
    
@outputSchema('length:int')
def num_chars(word):   
    """   
    Return the length of the provided word   
    """   
    return len(word)
```
示例3-8中的Pig脚本加载文本文件，并将reverse()和num_chars() Python函数应用于每个唯一的单词。

**Example 3-8. pig/playing_with_words.pig**
```
REGISTER  'udfs/string_funcs.py'  USING  streaming_python  AS string_udf;

-- Load the data from the file system
records = LOAD '../resources/input.txt';

-- Split each line of text and eliminate nesting
terms = FOREACH records GENERATE FLATTEN(TOKENIZE((chararray) $0)) AS word;

-- Group similar terms
grouped_terms = GROUP terms BY word;

-- Count the number of tuples in each group
unique_terms = FOREACH grouped_terms GENERATE group as word;

-- Calculate the number of characters in each term
term_length  =  FOREACH  unique_terms  GENERATE  word, string_udf.num_chars(word) as length;

-- Display the terms and their length
DUMP term_length;

-- Reverse each word  
reverse_terms  =  FOREACH  unique_terms  GENERATE  word, string_udf.reverse(word) as reverse_word;

-- Display the terms and the reverse terms
DUMP reverse_terms;
```
使用以下命令执行脚本（显示示例输出）：

```
$ pig -x local playing_with_words.pig
...
(be,2)
(the,3)
(jack,4)
(over,4)
(quick,5)
(jumped,6)
(nimble,6)
(candlestick,11)
...
(be,eb)
(the,eht)
(jack,kcaj)
(over,revo)
(quick,kciuq)
(jumped,depmuj)
(nimble,elbmin)
(candlestick,kcitseldnac)
```
## Most Recent Movies
以下示例使用groupLens数据集和外部库中的电影数据来计算最近的10部电影。

示例3-9中的Python UDF包含两个函数：parse_title()和days_since_release()。 parse_title()函数使用Python的正则表达式模块从电影的标题中删除发行年份。 days_since_release()函数使用datetime模块计算当天与电影发布日期之间的天数。

**Example 3-9. pig/udfs/movies_udf.py**

```
from pig_util import outputSchema
from datetime import datetime
import re

@outputSchema('title:chararray')
def parse_title(title):   
    """   
    Return the title without the year   
    """   
    return re.sub(r'\s*\(\d{4}\)','', title)

@outputSchema('days_since_release:int')
def days_since_release(date):   
    """   
    Calculate the number of days since the titles release   
    """   
    if date is None:      
        return None   
    
    today = datetime.today()   
    release_date = datetime.strptime(date, '%d-%b-%Y')   
    delta = today - release_date   
    return delta.days
```
例3-10中的Pig脚本使用Python UDF来确定最近的10部电影。

**Example 3-10. pig/recent_movies.pig**

```
REGISTER 'udfs/movies_udf.py' USING streaming_python AS movies_udf;

-- Load the data from the file system
records = LOAD '../resources/movies' USING PigStorage('|')    AS (id:int, title:chararray, release_date:chararray);

-- Parse the titles and determine how many days since the release date
titles  =  FOREACH  records  GENERATE  movies_udf.parse_title(title), movies_udf.days_since_release(release_date);

-- Order the movies by the time since release
most_recent = ORDER titles BY days_since_release ASC;

-- Get the ten most recent movies
top_ten = LIMIT most_recent 10;

-- Display the top ten most recent movies
DUMP top_ten;
```
以下命令用于执行脚本（显示示例输出）：

```
$ pig -x local recent_movies.pig 
...
(unknown,)
(Apt Pupil,6183)
(Mighty, The,6197)
(City of Angels,6386)
(Big One, The,6393)
(Lost in Space,6393)
(Mercury Rising,6393)
(Spanish Prisoner, The,6393)
(Hana-bi,6400)
(Object of My Affection, The,6400)
```
# (章节总结) Chapter Summary

本章介绍了Pig and Pig Latin。 它描述了Pig Latin的基本概念，允许创建和执行简单的Pig脚本。 它还介绍了如何使用Python UDF扩展Pig Latin的功能。


