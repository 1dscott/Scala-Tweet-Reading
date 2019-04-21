
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._


object PrintTweets {
  def main(args: Array[String]): Unit = {
    //set up twitter credentials
    setupTwitter()

    //setup spark streaming contect names the runs locally
    //using all CPUcores and five second batches of data
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(5))

    //get rid of log spam
    setupLogging()

    //create a DStream from twitter using our streaming context
    //DStream is object representing batches of data from the user
    val tweetsDStream = TwitterUtils.createStream(ssc, None)

    //now extract the text of each status into RDDs usingmap
    val statusesDStream = tweetsDStream.map(status => status.getText())

    //print out the first 10 of each RDD (each batch)
    statusesDStream.print()

    //kick it all off
    ssc.start()
    ssc.awaitTermination()

  }
}
