//determine the most popular word for given window
//analyzes tweets

import Utilities.{setupLogging, setupTwitter}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object FPhw4 {
  def main(args: Array[String]): Unit = {

    //set up twitter credentials
    setupTwitter()

    //setup spark streaming contect names the runs locally
    //using all CPUcores and five second batches of data
    val ssc = new StreamingContext("local[*]", "FPhw4", Seconds(5))

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

    //match each word to a key/value pair of (tag, 1)
    //so we can count them up by adding up the values
    val wordsDStreamPopular = wordsDStream.map(tag => (tag,1))

    //count them up over a 5 minute window and slaide every 5 seconds
    val wordCounts = wordsDStreamPopular.reduceByKeyAndWindow((x,y) => x + y, (x,y) => x-y, Seconds(300), Seconds(5))

    //sort results by count value
    val sortedResults = wordCounts.transform(rdd => rdd.sortBy(x => x._2, false))

    //print all the most popular words currently tweeted
    sortedResults.print()

    //create a folder and place all the tweets inside text files inside the folder (part 2)
    sortedResults.saveAsTextFiles("../data/Most Popular Words Tweeted.txt")

    //we need to set a directory to save our current status of tweeting app
    //purpse of the checkpoint is to recover from failure in case you server is down
    ssc.checkpoint("../data/checkpoint")
    ssc.start()
    ssc.awaitTermination()


  }
}
