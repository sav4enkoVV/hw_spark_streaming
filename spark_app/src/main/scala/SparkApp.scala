import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, window, from_json}
import org.apache.spark.sql.types.{StructType, IntegerType}

// https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
object SparkApp extends App {

  val spark = SparkSession.builder
    .appName("spark-streaming-hw")
    .master(sys.env.getOrElse("spark.master", "local[*]"))
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val brokers = "localhost:9092"
  val topic = "records"

  val records: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", topic)
    .load()

  import spark.implicits._

  val schema = new StructType()
    .add("engineTemp", IntegerType)

  val dfJson = records
    .withColumn("data", from_json(col("value").cast("String"), schema))
    .select("timestamp", "data.engineTemp")

  val windowedCounts = dfJson
    .groupBy(window($"timestamp", "10 seconds", "10 seconds"))
    .avg("engineTemp")

  val query = windowedCounts.writeStream
    .outputMode("update")
    .format("console")
    .start()

  query.awaitTermination()

}
