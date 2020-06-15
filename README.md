# spark-training

## wikipedia task
To start, first download the assignment: wikipedia.zip. For this assignment, you also need to download the data (133 MB):

http://alaska.epfl.ch/~dockermoocs/bigdata/wikipedia.dat

and place it in the folder: src/main/resources/wikipedia in your project directory.

In this assignment, you will get to know Spark by exploring full-text Wikipedia articles.

Gauging how popular a programming language is important for companies judging whether or not they should adopt an emerging programming language. For that reason, industry analyst firm RedMonk has bi-annually computed a ranking of programming language popularity using a variety of data sources, typically from websites like GitHub and StackOverflow. See their top-20 ranking for June 2016 as an example.

In this assignment, we'll use our full-text data from Wikipedia to produce a rudimentary metric of how popular a programming language is, in an effort to see if our Wikipedia-based rankings bear any relation to the popular Red Monk rankings.

You'll complete this exercise on just one node (your laptop), but you can also head over to Databricks Community Edition to experiment with your code on a "micro-cluster" for free.
Set up Spark

For the sake of simplified logistics, we'll be running Spark in "local" mode. This means that your full Spark application will be run on one node, locally, on your laptop.

To start, we need a SparkContext. A SparkContext is the "handle" to your cluster. Once you have a SparkContext, you can use it to create and populate RDDs with data.

To create a SparkContext, you need to first create a SparkConfig instance. A SparkConfig represents the configuration of your Spark application. It's here that you must specify that you intend to run your application in "local" mode. You must also name your Spark application at this point. For help, see the Spark API Docs.

Configure your cluster to run in local mode by implementing val conf and val sc.
Read-in Wikipedia Data

There are several ways to read data into Spark. The simplest way to read in data is to convert an existing collection in memory to an RDD using the parallelize method of the Spark context.

We have already implemented a method parse in the object WikipediaData object that parses a line of the dataset and turns it into a WikipediaArticle.

Create an RDD (by implementing val wikiRdd) which contains the WikipediaArticle objects of articles.
Compute a ranking of programming languages

We will use a simple metric for determining the popularity of a programming language: the number of Wikipedia articles that mention the language at least once.
Rank languages attempt #1: rankLangs

Computing occurrencesOfLang

Start by implementing a helper method occurrencesOfLang which computes the number of articles in an RDD of type RDD[WikipediaArticles] that mention the given language at least once. For the sake of simplicity we check that it least one word (delimited by spaces) of the article text is equal to the given language.

Computing the ranking, rankLangs

Using occurrencesOfLang, implement a method rankLangs which computes a list of pairs where the second component of the pair is the number of articles that mention the language (the first component of the pair is the name of the language).

An example of what rankLangs might return might look like this, for example:

The list should be sorted in descending order. That is, according to this ranking, the pair with the highest second component (the count) should be the first element of the list.

Pay attention to roughly how long it takes to run this part! (It should take tens of seconds.)
Rank languages attempt #2: rankLangsUsingIndex

Compute an inverted index

An inverted index is an index data structure storing a mapping from content, such as words or numbers, to a set of documents. In particular, the purpose of an inverted index is to allow fast full text searches. In our use-case, an inverted index would be useful for mapping from the names of programming languages to the collection of Wikipedia articles that mention the name at least once.

To make working with the dataset more efficient and more convenient, implement a method that computes an "inverted index" which maps programming language names to the Wikipedia articles on which they occur at least once.

Implement method makeIndex which returns an RDD of the following type: RDD[(String, Iterable[WikipediaArticle])]. This RDD contains pairs, such that for each language in the given langs list there is at most one pair. Furthermore, the second component of each pair (the Iterable) contains the WikipediaArticles that mention the language at least once.

Hint: You might want to use methods flatMap and groupByKey on RDD for this part.

Computing the ranking, rankLangsUsingIndex

Use the makeIndex method implemented in the previous part to implement a faster method for computing the language ranking.

Like in part 1, rankLangsUsingIndex should compute a list of pairs where the second component of the pair is the number of articles that mention the language (the first component of the pair is the name of the language).

Again, the list should be sorted in descending order. That is, according to this ranking, the pair with the highest second component (the count) should be the first element of the list.

Hint: method mapValues on PairRDD could be useful for this part.

Can you notice a performance improvement over attempt #2? Why?
Rank languages attempt #3: rankLangsReduceByKey

In the case where the inverted index from above is only used for computing the ranking and for no other task (full-text search, say), it is more efficient to use the reduceByKey method to compute the ranking directly, without first computing an inverted index. Note that the reduceByKey method is only defined for RDDs containing pairs (each pair is interpreted as a key-value pair).

Implement the rankLangsReduceByKey method, this time computing the ranking without the inverted index, using reduceByKey.

Like in part 1 and 2, rankLangsReduceByKey should compute a list of pairs where the second component of the pair is the number of articles that mention the language (the first component of the pair is the name of the language).

Again, the list should be sorted in descending order. That is, according to this ranking, the pair with the highest second component (the count) should be the first element of the list.

Can you notice an improvement in performance compared to measuring both the computation of the index and the computation of the ranking as we did in attempt #2? If so, can you think of a reason?

## stackoverflow data link
To start, first download the assignment: stackoverflow.zip. For this assignment, you also need to download the data (170 MB):

http://alaska.epfl.ch/~dockermoocs/bigdata/stackoverflow.csv

and place it in the folder: src/main/resources/stackoverflow in your project directory.

The overall goal of this assignment is to implement a distributed k-means algorithm which clusters posts on the popular question-answer platform StackOverflow according to their score. Moreover, this clustering should be executed in parallel for different programming languages, and the results should be compared.

The motivation is as follows: StackOverflow is an important source of documentation. However, different user-provided answers may have very different ratings (based on user votes) based on their perceived value. Therefore, we would like to look at the distribution of questions and their answers. For example, how many highly-rated answers do StackOverflow users post, and how high are their scores? Are there big differences between higher-rated answers and lower-rated ones?

Finally, we are interested in comparing these distributions for different programming language communities. Differences in distributions could reflect differences in the availability of documentation. For example, StackOverflow could have better documentation for a certain library than that library's API documentation. However, to avoid invalid conclusions we will focus on the well-defined problem of clustering answers according to their scores.

Note: for this assignment, we assume you recall the K-means algorithm introduced during Parallel Programming part of the specialization. You may refer back to the K-means assignment text for an overview of the algorithm!
The Data

You are given a CSV (comma-separated values) file with information about StackOverflow posts. Each line in the provided text file has the following format:

A short explanation of the comma-separated fields follows.

You will see the following code in the main class:

It corresponds to the following steps:

    lines: the lines from the csv file as strings
    raw: the raw Posting entries for each line
    grouped: questions and answers grouped together
    scored: questions and scores
    vectors: pairs of (language, score) for each question

The first two methods are given to you. You will have to implement the rest.
Data processing

We will now look at how you process the data before applying the kmeans algorithm.
Grouping questions and answers

The first method you will have to implement is groupedPostings:

In the raw variable we have simple postings, either questions or answers, but in order to use the data we need to assemble them together. Questions are identified using a postTypeId == 1. Answers to a question with id == QID have (a) postTypeId == 2 and (b) parentId == QID.

Ideally, we want to obtain an RDD with the pairs of (Question, Iterable[Answer]). However, grouping on the question directly is expensive (can you imagine why?), so a better alternative is to match on the QID, thus producing an RDD[(QID, Iterable[(Question, Answer))].

To obtain this, in the groupedPostings method, first filter the questions and answers separately and then prepare them for a join operation by extracting the QID value in the first element of a tuple. Then, use one of the join operations (which one?) to obtain an RDD[(QID, (Question, Answer))]. Then, the last step is to obtain an RDD[(QID, Iterable[(Question, Answer)])]. How can you do that, what method do you use to group by the key of a pair RDD?

Finally, in the description we used QID, Question and Answer types, which we've defined as type aliases for Postings and Ints. The full list of type aliases is available in package.scala:

The above information should allow you to implement the groupedPostings method. Its signature is:

Computing Scores

Second, implement the scoredPostings method, which should return an RDD containing pairs of (a) questions and (b) the score of the answer with the highest score (note: this does not have to be the answer marked as acceptedAnswer!). The type of this scored RDD is:

For example, the scored RDD should contain the following tuples:

Hint: use the provided answerHighScore given in scoredPostings.
Creating vectors for clustering

Next, we prepare the input for the clustering algorithm. For this, we transform the scored RDD into a vectors RDD containing the vectors to be clustered. In our case, the vectors should be pairs with two components (in the listed order!):

    Index of the language (in the langs list) multiplied by the langSpread factor.
    The highest answer score (computed above).

The langSpread factor is provided (set to 50000). Basically, it makes sure posts about different programming languages have at least distance 50000 using the distance measure provided by the euclideanDist function. You will learn later what this distance means and why it is set to this value.

The type of the vectors RDD is as follows:

For example, the vectors RDD should contain the following tuples:

Implement this functionality in method vectorPostings by using the given firstLangInTag helper method.

(Idea for test: scored RDD should have 2121822 entries)
Kmeans Clustering

Based on these initial means, and the provided variables converged method, implement the K-means algorithm by iteratively:

    pairing each vector with the index of the closest mean (its cluster);
    computing the new means by averaging the values of each cluster.

To implement these iterative steps, use the provided functions findClosest, averageVectors, and euclideanDistance.

Note 1:

In our tests, convergence is reached after 44 iterations (for langSpread = 50000) and in 104 iterations (for langSpread = 1), and for the first iterations the distance kept growing. Although it may look like something is wrong, this is the expected behavior. Having many remote points forces the kernels to shift quite a bit and with each shift the effects ripple to other kernels, which also move around, and so on. Be patient, in 44 iterations the distance will drop from over 100000 to 13, satisfying the convergence condition.

If you want to get the results faster, feel free to downsample the data (each iteration is faster, but it still takes around 40 steps to converge):

However, keep in mind that we will test your assignment on the full data set. So that means you can downsample for experimentation, but make sure your algorithm works on the full data set when you submit for grading.

Note 2:

The variable langSpread corresponds to how far away are languages from the clustering algorithm's point of view. For a value of 50000, the languages are too far away to be clustered together at all, resulting in a clustering that only takes scores into account for each language (similarly to partitioning the data across languages and then clustering based on the score). A more interesting (but less scientific) clustering occurs when langSpread is set to 1 (we can't set it to 0, as it loses language information completely), where we cluster according to the score. See which language dominates the top questions now?
Computing Cluster Details

After the call to kmeans, we have the following code in method main:

Implement the clusterResults method, which, for each cluster, computes:

    (a) the dominant programming language in the cluster;
    (b) the percent of answers that belong to the dominant language;
    (c) the size of the cluster (the number of questions it contains);
    (d) the median of the highest answer scores.

Once this value is returned, it is printed on the screen by the printResults method.
Questions

    Do you think that partitioning your data would help?
    Have you thought about persisting some of your data? Can you think of why persisting your data in memory may be helpful for this algorithm?
    Of the non-empty clusters, how many clusters have "Java" as their label (based on the majority of questions, see above)? Why?
    Only considering the "Java clusters", which clusters stand out and why?
    How are the "C# clusters" different compared to the "Java clusters"?

Hint: if you break the grader's time or memory constraints, think of how partitioning or persisting could, if at all, help you gain some performance. Please note that our grader only runs unit tests against your methods. It won't run the main method, so make sure to place any caching or partitioning code outside the main. 



## Timeusage task
To start, first download the assignment: timeusage.zip. For this assignment, you also need to download the data (164 MB):

http://alaska.epfl.ch/~dockermoocs/bigdata/atussum.csv

and place it in the folder src/main/resources/timeusage/ in your project directory.
The problem

The dataset is provided by Kaggle and is documented here:

https://www.kaggle.com/bls/american-time-use-survey

The file uses the comma-separated values format: the first line is a header defining the field names of each column, and every following line contains an information record, which is itself made of several columns. It contains information about how people spend their time (e.g., sleeping, eating, working, etc.).

Here are the first lines of the dataset:

Our goal is to identify three groups of activities:

    primary needs (sleeping and eating),
    work,
    other (leisure).

And then to observe how do people allocate their time between these three kinds of activities, and if we can see differences between men and women, employed and unemployed people, and young (less than 22 years old), active (between 22 and 55 years old) and elder people.

At the end of the assignment we will be able to answer the following questions based on the dataset:

    how much time do we spend on primary needs compared to other activities?
    do women and men spend the same amount of time in working?
    does the time spent on primary needs change when people get older?
    how much time do employed people spend on leisure compared to unemployed people?

To achieve this, we will first read the dataset with Spark, transform it into an intermediate dataset which will be easier to work with for our use case, and finally compute information that will answer the above questions.
Read-in Data

The simplest way to create a DataFrame consists in reading a file and letting Spark-sql infer the underlying schema. However this approach does not work well with CSV files, because the inferred column types are always String.

In our case, the first column contains a String value identifying the respondent but all the other columns contain numeric values. Since this schema will not be correctly inferred by Spark-sql, we will define it programmatically. However, the number of columns is huge. So, instead of manually enumerating all the columns we can rely on the fact that, in the CSV file, the first line contains the name of all the columns of the dataset.

Our first task consists in turning this first line into a Spark-sql StructType. This is the purpose of the dfSchema method. This method returns a StructType describing the schema of the CSV file, where the first column has type StringType and all the others have type DoubleType. None of these columns are nullable.

The second step is to be able to effectively read the CSV file is to turn each line into a Spark-sql Row containing columns that match the schema returned by dfSchema. That’s the job of the row method.

Project

As you probably noticed, the initial dataset contains lots of information that we don’t need to answer our questions, and even the columns that contain useful information are too detailed. For instance, we are not interested in the exact age of each respondent, but just whether she was “young”, “active” or “elder”.

Also, the time spent on each activity is very detailed (there are more than 50 reported activities). Again, we don’t need this level of detail; we are only interested in three activities: primary needs, work and other.

So, with this initial dataset it would a bit hard to express the queries that would give us the answers we are looking for.

The second part of this assignment consists in transforming the initial dataset into a format that will be easier to work with.

A first step in this direction is to identify which columns are related to the same activity. Based on the description of the activity corresponding to each column (given in this document), we deduce the following rules:

    “primary needs” activities (sleeping, eating, etc.) are reported in columns starting with “t01”, “t03”, “t11”, “t1801” and “t1803” ;
    working activities are reported in columns starting with “t05” and “t1805” ;
    other activities (leisure) are reported in columns starting with “t02”, “t04”, “t06”, “t07”, “t08”, “t09”, “t10”, “t12”, “t13”, “t14”, “t15”, “t16” and “t18” (only those which are not part of the previous groups).

Then our work consists in implementing the classifiedColumns method, which classifies the given list of column names into three Column groups (primary needs, work or other). This method should return a triplet containing the “primary needs” columns list, the “work” columns list and the “other” columns list.

The second step is to implement the timeUsageSummary method, which projects the detailed dataset into a summarized dataset. This summary will contain only 6 columns: the working status of the respondent, his sex, his age, the amount of daily hours spent on primary needs activities, the amount of daily hours spent on working and the amount of daily hours spent on other activities.

Each “activity column” will contain the sum of the columns related to the same activity of the initial dataset. Note that time amounts are given in minutes in the initial dataset, whereas in our resulting dataset we want them to be in hours.

The columns describing the work status, the sex and the age, will contain simplified information compared to the initial dataset.

Last, people that are not employable will be filtered out of the resulting dataset.

The comment on top of the timeUsageSummary method will give you more specific information about what is expected in each column.
Aggregate

Finally, we want to compare the average time spent on each activity, for all the combinations of working status, sex and age.

We will implement the timeUsageGrouped method which computes the average number of hours spent on each activity, grouped by working status (employed or unemployed), sex and age (young, active or elder), and also ordered by working status, sex and age. The values will be rounded to the nearest tenth.

Now you can run the project and see what the final DataFrame contains. What do you see when you compare elderly men versus elderly women's time usage? How much time elder people allocate to leisure compared to active people? How much time do active employed people spend to work?
Alternative ways to manipulate data

We can also implement the timeUsageGrouped method by using a plain SQL query instead of the DataFrame API. Note that sometimes using the programmatic API to build queries is a lot easier than writing a plain SQL query. If you do not have experience with SQL, you might find these examples useful.

Can you think of a previous query that would have been a nightmare to write in plain SQL?

Finally, in the last part of this assignment we will explore yet another alternative way to express queries: using typed Datasets instead of untyped DataFrames.

Implement the timeUsageSummaryTyped method to convert a DataFrame returned by timeUsageSummary into a DataSet[TimeUsageRow]. The TimeUsageRow is a data type that models the content of a row of a summarized dataset. To achieve the conversion you might want to use the getAs method of Row. This method retrieves a named column of the row and attempts to cast its value to a given type.

Then, implement the timeUsageGroupedTyped method that performs the same query as timeUsageGrouped but uses typed APIs as much as possible. Note that not all the operations have a typed equivalent. round is an example of operations that has no typed equivalent: it will return a Column that you will have to turn into a TypedColumn by calling .as[Double]. Another example is orderBy, which has no typed equivalent. Make sure your Dataset has a schema because this operation requires one (column names are generally lost when using typed transformations).

