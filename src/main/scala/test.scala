import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\Hadoop3") //This command is used for resolving the permission related exceptions w.r.t winutils.exe file.
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")
    var dfload = spark.read.csv("hdfs://localhost:9000/user/input/CarRental.csv")
    dfload.createOrReplaceTempView("car")
    dfload.show()
    spark.sql("SELECT * FROM car").show()

    //val driver = com.mysql.jdbc.driver
    val url = "jdbc:mysql://localhost:3306/p1"
    val user = "root"
    val pass = "N83ngmyun46$^"

    val sourceDF = spark.read.format("jdbc").option("url", url)
      .option("dbtable", "test1").option("user", user)
      .option("password", pass).load()
    sourceDF.show()

    spark.sql("DROP TABLE IF EXISTS rental")
    spark.sql("CREATE TABLE IF NOT EXISTS rental(fuelType String, rating String, renterTripsTaken String, reviewCount String, city String, state String, owner_id String, dailyRate String, make String, model String, type String, year String)row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/CarRental.csv' OVERWRITE INTO TABLE rental");
    spark.sql("SELECT Count(*) FROM rental").show()
    spark.sql("SELECT * FROM rental").show()



  }
}
