import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.SparkSession
import org.mongodb.scala._
import Helpers._
import org.mongodb.scala.model.Indexes
import com.mongodb.client.model.Filters
import com.mongodb.spark._
import org.apache.spark.sql.functions.pow
import org.bson.Document
import java.time.LocalDate

object Main {
  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("MongoDB")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/hw2.tweets")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/hw2.tweets")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val mongoClient: MongoClient = MongoClient()
    val database: MongoDatabase = mongoClient.getDatabase("hw2")
    val collection: MongoCollection[Document] = database.getCollection("tweets")

    // STEP 1:
    val dfRead = spark.read.format("json").load("src/boulder_flood_geolocated_tweets.json")
    dfRead.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite")
      .option("database" , "hw2")
      .option("collection" , "tweets")
      .save()


   // STEP 2:
    val pipeline = "{ '$set': { 'created_at': { '$toDate': '$created_at' } }}"
    val updateDate = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("pipeline", pipeline).load()
    updateDate.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()


    //STEP 3:
    collection.createIndex(Indexes.geo2dsphere("coordinates.coordinates")).printResults()


    //STEP 4:

    //values
    val word = args(1).toLowerCase()
    val radius = args(2).toDouble
    val long = args(3).toDouble
    val lat = args(4).toDouble
    val startDate = args(5)
    val endDate = args(6)


    //1 : using MongoSpark, by collecting tweets and filtering them spatio-temporally using dataframe apis
    val df = MongoSpark.load(spark)
    val filterDF = df
      .filter(s"created_at between '${startDate}' and '${endDate}'")
      .filter(pow(df("coordinates.coordinates")(0) - long,2) + pow(df("coordinates.coordinates")(1) - lat,2) <= radius * radius)

  try {
    val resultDF = filterDF.select("text")
      .rdd
      .map(f => f.toString)
      .map(f => f.split(" ").toList)
      .map(f => f.count(_.toLowerCase() == word))
      .reduce(_ + _)
    println(s"the word (${word}) repeated in geo location [$long , $lat] with radius [$radius] between $startDate and $endDate is: " + resultDF)
  }
  catch {
    case _: Throwable => println(s"the word (${word}) repeated in geo location [$long , $lat] with radius [$radius] between $startDate and $endDate is: 0")
  }


    //2 : using mongodb library by sending a normal mongoDB query to filter by time and space.
    val filterDB =  collection.find(Filters.and(Filters.geoWithinCenter("coordinates.coordinates",long,lat ,radius),
      Filters.gte("created_at" ,LocalDate.parse(startDate)),
      Filters.lte("created_at" ,LocalDate.parse(endDate)),
    )).results()

  try {
    val resultDB = filterDB
      .map(f => f.get("text").toString.replaceAll("Some[(]BsonString[{]value=", " "))
      .map(f => f.split(" ").toList)
      .map(f => f.count(_.toLowerCase() == word))
      .reduce(_ + _)
    println(s"the word (${word}) repeated in geo location [$long , $lat] with radius [$radius] between $startDate and $endDate is: " + resultDB)
  }
  catch {
    case _: Throwable => println(s"the word (${word}) repeated in geo location [$long , $lat] with radius [$radius] between $startDate and $endDate is: 0")
  }


    //3 : text index but I didn't use it
    collection.createIndex(Indexes.text("text")).printResults()
  }
}