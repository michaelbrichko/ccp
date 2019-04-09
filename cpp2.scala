import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

object cp {

  def main(args: Array[String]) {

    val isLocal = args(0).toBoolean;  // run locally on windows or on hdfs in the cloud

    if (isLocal) {
      System.setProperty("hadoop.home.dir", "C:/hadoop")
    }

    // Create a SparkContext to initialize Spark
    val conf = new SparkConf().setAppName("cp2")

    if (isLocal) {
      conf.setMaster("local[*]")
    } else {
      conf.setMaster("yarn")
    }

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.sparkContext.hadoopConfiguration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    ssc.sparkContext.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    var inputPath = ""
    var outputPath = ""

    if (isLocal) {
      inputPath  = "C:/cloud_computing_project_2/src/main/resources/input/"
      outputPath = "C:/cloud_computing_project_2/src/main/resources/output/"
    } else {
      inputPath = "hdfs:///input/"
      outputPath = "hdfs:///output/"
    }

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val input = ssc.textFileStream(inputPath).flatMap(input => input.split("\r\n"))


    // g2q1: For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X.
    input.map(line => (line.split(",")(2), line.split(",")(1), line.split(",")(6), line.split(",")(8))) // fromAirport, carrier, depDelay, isCancelled

      .filter(_._3.length > 0)      // filter out rows where depDelay is not present
      .filter(_._4.toInt == 0)      // filter out cancelled flights
      .map(t => (t._1, t._2, t._3)) // remove cancelled flights column

      .foreachRDD(rdd => rdd.groupBy(t => t._1 + "-" + t._2) // for every airport-carrier calculate average delay
          .map(t => (t._2.toList.head._1, t._2.toList.head._2, t._2.toList.map(t => t._3.toDouble).sum / t._2.toList.length))

          .groupBy(t => t._1) // group by airport
          .map(g => (g._1, g._2.map(s => (s._2, s._3))
          .toList.sortWith(_._2.toDouble > _._2.toDouble).take(10) // sort by departure delay and take first 10
          .map(t => t._1 + ":" + t._2).mkString(" ")))
          .map(t => t._1 + "," + t._2)

          .saveAsTextFile(outputPath + "g2q1"))


    // g2q2: For each airport X, rank the top-10 airports in decreasing order of on-time departure performance from X.
    input.map(line => (line.split(",")(2), line.split(",")(3), line.split(",")(6), line.split(",")(8))) // fromAirport, toAirport, depDelay, isCancelled

      .filter(_._3.length > 0)      // filter out rows where depDelay is not present
      .filter(_._4.toInt == 0)      // filter out cancelled flights
      .map(t => (t._1, t._2, t._3)) // remove cancelled flights column

      .foreachRDD(rdd => rdd.groupBy(t => t._1 + "-" + t._2) // for every airport-carrier calculate average delay
          .map(t => (t._2.toList.head._1, t._2.toList.head._2, t._2.toList.map(t => t._3.toDouble).sum / t._2.toList.length))

          .groupBy(t => t._1) // group by airport
          .map(g => (g._1, g._2.map(s => (s._2, s._3))
          .toList.sortWith(_._2.toDouble > _._2.toDouble).take(10) // sort by departure delay and take first 10
          .map(t => t._1 + ":" + t._2).mkString(" ")))
          .map(t => t._1 + "," + t._2)

          .saveAsTextFile(outputPath + "g2q2"))


    //g2q3: For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.
    input.map(line => (line.split(",")(2), line.split(",")(3), line.split(",")(1), line.split(",")(7), line.split(",")(8))) // fromAirport, toAirport, carrier, arrivalDelay, isCancelled

      .filter(_._4.length > 0)            // filter out rows where arrivalDelay is not present
      .filter(_._5.toInt == 0)            // filter out cancelled flights
      .map(t => (t._1, t._2, t._3, t._4)) // remove cancelled flights column

      .foreachRDD(rdd => rdd.groupBy(t => t._1 + "-" + t._2) // group by fromAirport-toAirport
          .map(t => (t._2.toList.head._1 + "-" + t._2.toList.head._2, t._2.toList.map(t => (t._3, t._4))
          .groupBy(_._1) // group by carrier
          .map(t => (t._1, t._2.map(cd => cd._2.toDouble).sum / t._2.length)).toList
          .sortWith(_._2.toDouble > _._2.toDouble).take(10) // sort by arrival delay and take first 10
          .map(t => t._1 + ":" + t._2).mkString(" ")))
          .map(t => t._1 + "," + t._2)

          .saveAsTextFile(outputPath + "g2q3"))


         // g3q2: Tom wants to travel from airport X to airport Z. However, Tom also wants to stop at airport Y for some sightseeing on the way. More concretely, Tom has the following requirements:
         // a) The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y).
         // b) Tom wants his flights scheduled to depart airport X before 12:00 PM local time and to depart airport Y after 12:00 PM local time.
         // c) Tom wants to arrive at each destination with as little delay as possible. You can assume you know the actual delay of each flight.
         val baseInput = input.map(line =>
             (line.split(",")(2),  // 1. fromAirport
              line.split(",")(3),  // 2. toAirport
              line.split(",")(5),  // 3. departureTime
              line.split(",")(4),  // 4. departureDate
              line.split(",")(7),  // 5. arrivalDelay
              line.split(",")(8))) // 6. isCancelled

              .filter(_._5.length > 0)         // filter out rows where arrivalDelay is not present
              .filter(_._6.toInt == 0)         // filter out cancelled flights
              .filter(_._4.startsWith("2008")) // filter out year != 2008

              .map(t => (t._1, t._2, t._3, t._4, t._5)) // remove cancelled flights column


          baseInput.transformWith(baseInput, (rddA: RDD[(String,String,String,String,String)], rddB: RDD[(String,String,String,String,String)]) => rddA.cartesian(rddB)) // cross join

              .filter(_._1._3.toInt < 1200)                    // x->y journeys
              .filter(_._2._3.toInt > 1200)                    // y->z journeys
              .filter { case (xy, yz) => xy._2.equals(yz._1) } // xy toAirport should be equal to yz fromAirport
              .filter { case (xy, yz) => DateTime.parse(xy._4).plusDays(2).toString("yyyy-MM-dd").equals(yz._4) } // filter out flights that do not occur 2 days apart

              .map(t => (t._1._1 + "-" + t._1._2 + "-" + t._2._2 + "-" + t._1._4,    t._1._3, t._1._5, t._2._4, t._2._3, t._2._5))

              .foreachRDD(rdd =>
                  rdd.groupBy(_._1).map(t => (t._1, t._2.map(f => (f._2, f._3, f._4, f._5, f._6)).toList
                    .sortWith { case (r1, r2) => r1._2.toDouble + r1._5.toDouble < r2._2.toDouble + r2._5.toDouble }.take(1))) // sort by overall delay and take first
                    .map(t => t._1 + "," + t._2.map(e => e._1 + " " + e._2 + " " + e._3 + " " + e._4 + " " + e._5).mkString(" "))
                    .saveAsTextFile(outputPath + "g3q2"))


    ssc.start()            // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}