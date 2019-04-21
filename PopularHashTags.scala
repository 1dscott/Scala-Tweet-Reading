//listen to a stream of tweets and keep track of most popular hashtags over a 5 minute window

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._

object PopularHashTags {
  def main(args: Array[String]): Unit = {
    //set up twitter credentials
    setupTwitter()

    //setup spark streaming contect names the runs locally
    //using all CPUcores and five second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashTags", Seconds(5))

    setupLogging()

    //DStream is object representing batches of data from the user
    val tweetsDStream = TwitterUtils.createStream(ssc, None)

    //now extract the text of each status into RDDs usingmap
    val statusesDStream = tweetsDStream.map(status => status.getText())

    //extract the text of each status
    //each element contains text for each tweet
    val textDStream = tweetsDStream.map(status => status.getText())

    //get each word of text
    val wordsDStream = textDStream.flatMap(text => text.split(" "))

    //now eleminate anything that is not a hashtag
    val hashTagsDStream = wordsDStream.filter(word => word.startsWith("#"))

    //match each hashtag to a key/value pair of (hashtag, 1)
    //so we can count them up by adding up the values
    val hashtagKeyValues = hashTagsDStream.map(hashtag => (hashtag,1))

    //count them up over a 5 minute window and slaide every 5 seconds
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow((x,y) => x + y, (x,y) => x-y, Seconds(300), Seconds(5))

    //sort results by count value
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))

    sortedResults.print()

    //Let us check contents of textDSSTREAM
   /* textDStream.foreachRDD((rdd, time) => {
      println()
      println("sample data for textDStream")
      rdd.takeSample(false,3).foreach(println)
      println()
    })

    hashTagsDStream.foreachRDD((rdd,time) => {
      println()
      println("sample data for thashtags")
      rdd.takeSample(false,3).foreach(println)
      println()
    })*/

    //kick it off
    //we need to set a directory to save our current status of tweeting app
    //purpse of the checkpoint is to recover from failure in case you server is down
    ssc.checkpoint("../data/checkpoint")
    ssc.start()
    ssc.awaitTermination()

  }
}
