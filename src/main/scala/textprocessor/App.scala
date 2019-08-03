package textprocessor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.{SparkConf, SparkContext}

object App {
  def main(args : Array[String]) : Unit = {
    var inputPath = args(0)
    var outputPath = args(1)

//    var inputPath = "src/dataset"

    val configuration = new SparkConf().setAppName("Your Application Name").setMaster("local")
    val sc = new SparkContext(configuration)
    val spark = SparkSession.builder().appName("TextProcessor").getOrCreate()
    val df_twitter = spark.read.format("avro").option("header","true").load( inputPath + "/tweet.avro")

    // 1. Find top 10 users with highest tweets

    val df_twitter_active_users = df_twitter.groupBy(col("username")).agg(count("*")
      .alias("total_tweets"))
      .orderBy(desc("total_tweets")).limit(10);

    df_twitter.write.json(outputPath +"/output/exercise1/")

    // 2. Find top 10 persons mentioned on the tweets with count

   val df_user_mentioned = df_twitter.withColumn("tweet_split", split(col("tweet"), " "))
    .withColumn("tweet_split", explode(col("tweet_split")))
    .filter(col("tweet_split") rlike """(@\w+)""")

    val df_user_grouped = df_user_mentioned
    .groupBy(col("tweet_split"))
    .agg(count("*")
      .alias("total_mentions"))
    .orderBy(desc("total_mentions")).limit(10);

    df_user_grouped.write.json(outputPath +"/output/exercise2/")

    // 3.  Find top 5 days (e.g. [Mon Apr], [Tue May]) with maximum tweets

    val twitter_timestamp_format = "EEE MMM dd HH:mm:ss ZZZZZ yyyy"

    var df_twitter_cast = df_twitter.withColumn("timestamp", to_timestamp(col("timestamp"), twitter_timestamp_format));
    df_twitter_cast = df_twitter_cast.withColumn("date", col("timestamp").cast(DateType));

    val df_twitter_popular_days = df_twitter_cast.groupBy(col("date"))
      .agg(count("*")
      .alias("total_tweets"))
      .orderBy(desc("total_tweets")).limit(10);

    df_twitter_popular_days.write.json(outputPath +"/output/exercise3/")

    // 4. Find top 10 words from wordcount.txt against the tweets except these stop words:
    // [to, the, a, and, is, for, in, of, all]

    val banned_words = Seq("to", "the", "a", "and", "is", "for", "in", "of", "all")

    val file_df = spark.read.text(inputPath +"/wordcount.txt")

    val words_explode = file_df.withColumn("value", split(col("value"), " "))
      .withColumn("value", explode(col("value")))
      .filter(!col("value").isin(banned_words  : _*))
        .select("value").distinct

    val df_total_words = df_twitter.withColumn("tweet_split", split(col("tweet"), " "))
      .withColumn("tweet_split", explode(col("tweet_split"))).select("tweet_split")

    val df_joined_words = df_total_words.join(words_explode, words_explode.col("value") === df_total_words.col("tweet_split"), "inner")
      .select(col("tweet_split").alias("words"))
      .groupBy("words")
      .agg(count("*")
      .alias("total_number"))
      .orderBy(desc("total_number")).limit(10);
    df_joined_words.show()

    words_explode.write.json(outputPath +"/output/exercise4/")
  }
}
