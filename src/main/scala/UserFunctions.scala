import Main._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.sql.{Connection, DriverManager}
import scala.io.StdIn.{readInt, readLine}

object UserFunctions extends App {

  def sparkCn(f_name: String, l_name: String, username: String, password: String): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Hadoop3")
    val spark = SparkSession
      .builder
      .appName("Car Rental Query")
      .master("local[*]")
      .config("spark.master", "local[*]")
      .config("spark.driver.allowMultipleContexts", "true")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    println("created spark session")

    //connect to the database named "mysql" on the localhost
    val url = "jdbc:mysql://localhost:3306/p1"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val pass = "N83ngmyun46$^"
    var connection: Connection = null
    try {
      //make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, user, pass)
      //create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM test1;")
      while (resultSet.next()) {
        //println(resultSet.getString(1) + "," + resultSet.getString(2) + "," + resultSet.getString(3))
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      connection.close()
    }

  import spark.implicits._

  val df = Seq((f_name, l_name, username, password)).toDF("first_name", "last_name", "username", "password")
  df.show()

  val sourceDf = spark.read.format("jdbc").option("url", url)
    .option("dbtable", "test1").option("user", user)
    .option("password", pass).load()

  df.write.mode(SaveMode.Append).format("jdbc").option("url", url)
    .option("dbtable", "test1").option("user", user)
    .option("password", pass).save()

}

  def login(): Unit = {
    println("\nEnter your username: ")
    val username = readLine().toLowerCase
    println("\nEnter your password: ")
    val check_pass = readLine()

    val spark = SparkSession
      .builder
      .appName("Car Rental Query")
      .master("local[*]")
      .config("spark.master", "local[*]")
      .config("spark.driver.allowMultipleContexts", "true")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")

    val url = "jdbc:mysql://localhost:3306/p1"
    val user = "root"
    val pass = "N83ngmyun46$^"

    val sourceDf = spark.read.format("jdbc").option("url", url)
      .option("dbtable", "test1").option("user", user)
      .option("password", pass).load()

    sourceDf.createOrReplaceTempView("users1")
    spark.sql(s"SELECT * FROM users1 WHERE username = '$username' AND password = '$check_pass'")

    if (check_pass == "") {
      println("\nEnter Password: ")
    } else if (username == "") {
      println("\nEnter Username: ")
    } else if (spark.sql(s"SELECT * FROM users1 WHERE username = '$username' AND password = '$check_pass'").count() == 0) {
      println("\nInvalid Username or Password")
      login()
    } else {
      //create_rental()
      println("\nLogin Successful")
      println("\nWelcome " + username.toLowerCase().capitalize)
      user_page()
    }
  }

  def createAccount(): Unit = {
    println("\nEnter Your First Name: ")
    val f_name = readLine().toLowerCase
    println("\nEnter Your Last Name: ")
    val l_name = readLine().toLowerCase
    println("\nEnter A Username: ")
    val username = readLine().toLowerCase
    println("\nEnter A Password: ")
    val password = readLine()
    println("\nAccount Created Successfully")
  sparkCn(f_name, l_name, username, password)
  mainPage(Array())
  }

  def adminLogin(): Unit = {
    println("\nEnter Your Username: ")
    val username = readLine().toLowerCase
    println("\nEnter Your Password: ")
    val check_pass = readLine()
    if (username == "admin" || check_pass == "admin") {
    println("\nAdmin Login Successful")
    admin_page()
  }
    else {
      println("\nInvalid Username or Password")
      mainPage(Array())
      }
    }

  def admin_page(): Unit = {
    println("[1] Create Account\n")
    println("[2] Delete Account\n")
    println("[3] View Accounts\n")
    println("[4] Logout\n")
    val choice = readLine("\nEnter Your Selection: ")
    choice match {
      case "1" => createAccount()
      case "2" => //deleteAccount()
      println("\nNot Implemented\n")
      admin_page()
      case "3" => viewAccount()
      //println("Not Implemented")
      case "4" => mainPage(Array())
      case _ => println("\nInvalid Choice\n")
        admin_page()

    }
  }

  def viewAccount(): Unit = {
    val spark = SparkSession
      .builder
      .appName("Car Rental Query")
      .master("local[*]")
      .config("spark.master", "local[*]")
      .config("spark.driver.allowMultipleContexts", "true")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")

    val url = "jdbc:mysql://localhost:3306/p1"
    val user = "root"
    val pass = "N83ngmyun46$^"

    val sourceDf = spark.read.format("jdbc").option("url", url)
      .option("dbtable", "test1").option("user", user)
      .option("password", pass).load()

    sourceDf.createOrReplaceTempView("users1")
    spark.sql(s"SELECT * FROM users1").show()
    admin_page()
  }

  val spark = SparkSession
    .builder
    .appName("Car Rental Query")
    .master("local[*]")
    .config("spark.master", "local[*]")
    .config("spark.driver.allowMultipleContexts", "true")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)
  //Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  println("created spark session")


  def create_rental(): Unit = {
    val spark = SparkSession
      .builder
      .appName("Car Rental Query")
      .master("local[*]")
      .config("spark.master", "local[*]")
      .config("spark.driver.allowMultipleContexts", "true")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    println("created spark session")
    spark.sql("DROP TABLE IF EXISTS rental")
    spark.sql("CREATE TABLE IF NOT EXISTS rental(fuelType String, rating String, renterTripsTaken String, reviewCount String, city String, state String, owner_id String, dailyRate String, make String, model String, type String, year String)row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/CarRental.csv' INTO TABLE rental")
    spark.sql("SELECT * FROM rental")//.show()
    user_page()
  }

  def user_page(): Unit = {

    println("\n[1] Make A Query Selection")
    println("\n[2] Update Username")
    println("\n[3] Update Password")
    println("\n[4] Delete Account")
    println("\n[5] Logout")

    val choice = readLine("\nEnter Your Selection: ")
    choice match {
      case "1" => user_session()
      case "2" => //update_username()
      println("Not Implemented")
      user_page()
      case "3" => //update_password()
      println("Not Implemented\n")
      user_page()
      case "4" => //delete_account()
      println("Not Implemented\n")
      user_page()
      case "5" => mainPage(Array())
      case _ => println("Invalid Selection\n")
        user_page()
    }
  }
  def user_session(): Unit = {

    println("\nMake Your Query Selection: ")
    println("\n[1] What is the most popular make and model for car rental?")
    println("\n[2] What is the average cost of care rental in various major cities?")
    println("\n[3] Which city has the highest cost to rent a car?")
    println("\n[4] Which city has the lowest cost to rent a car?")
    println("\n[5] Do states have a preference for fuel type (gasoline, electric, or hybrid) when renting a car?")
    println("\n[6] What is the average daily rate based on fuel type?")
    println("\n[7] Return to Main Menu")



    val spark = SparkSession
      .builder
      .appName("Car Rental Query")
      .master("local[*]")
      .config("spark.master", "local[*]")
      .config("spark.driver.allowMultipleContexts", "true")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    println("created spark session")

    val choice = readLine("Enter Your Selection: ")
    choice match {
      case "1" => {
        spark.sql("DROP VIEW IF EXISTS popular_car")
        spark.sql(s"CREATE VIEW IF NOT EXISTS popular_car AS SELECT make, model, COUNT(*) AS Count FROM rental GROUP BY make, model ORDER BY Count DESC")
        spark.sql("SELECT * FROM popular_car").show()
        println("\nDo You Want To Save To A JSON file? [1] YES [2] NO")
        val save = readInt()
        save match{
          case 1 => {
            spark.sql("SELECT * FROM popular_car").write.json("output/popular_car.json")
            println("\npopular_car.json Saved")
          }
          case 2 => println("\nJSON File Not Saved")
          case _ => println("\nInvalid Selection")
        }
        println("\nDo You Want to Make More Query Selection? [1] YES [2] NO")
        val select = readInt()
        select match{
          case 1 => user_session()
          case 2 => user_page()
          case _ => println("\nInvalid Selection")
            user_session()
        }
      }
      case "2" => {
        println("\nEnter The City You Want To Query: ")
        val city = readLine()//.toLowerCase.capitalize
        spark.sql("DROP VIEW IF EXISTS avg_cost")
        spark.sql(s"CREATE VIEW IF NOT EXISTS avg_cost AS SELECT city, state, AVG(dailyRate) AS avg_cost FROM rental GROUP BY city, state")
        spark.sql(s"SELECT * FROM avg_cost WHERE city = '$city'").show()
        println("\nDo You Want To Save To A JSON file? [1] YES [2] NO")
        val save = readInt()
        save match{
          case 1 => {
            spark.sql("SELECT * FROM avg_cost").write.json("output/avg_cost.json")
            println("avg_cost.json Saved")
          }
          case 2 => println("\nJSON File Not Saved")
          case _ => println("\nInvalid Selection")
        }
        println("\nDo You Want to Make More Query Selection? [1] YES [2] NO")
        val select = readInt()
        select match{
          case 1 => user_session()
          case 2 => user_page()
          case _ => println("\nInvalid Selection")
            user_session()
        }
      }
      case "3" => {
        spark.sql("DROP VIEW IF EXISTS highest_cost")
        spark.sql(s"CREATE VIEW IF NOT EXISTS highest_cost AS SELECT city, state, AVG(dailyRate) AS avg_cost FROM rental GROUP BY city, state")
        spark.sql("SELECT * FROM highest_cost ORDER BY avg_cost DESC limit 20").show()
        println("\nDo You Want To Save To A JSON File? [1] YES [2] NO")
        val save = readInt()
        save match{
          case 1 => {
            spark.sql("SELECT * FROM highest_cost").write.json("output/highest_cost.json")
            println("highest_cost.json Saved")
          }
          case 2 => println("\nJSON File Not Saved")
          case _ => println("\nInvalid Selection")
        }
        println("\nDo You Want to Make More Query Selection? [1] YES [2] NO")
        val select = readInt()
        select match{
          case 1 => user_session()
          case 2 => user_page()
          case _ => println("\nInvalid Selection")
            user_session()
        }
      }
      case "4" => {
        spark.sql("DROP VIEW IF EXISTS lowest_cost")
        spark.sql(s"CREATE VIEW IF NOT EXISTS lowest_cost AS SELECT city, state, AVG(dailyRate) AS avg_cost FROM rental GROUP BY city, state")
        spark.sql("SELECT * FROM lowest_cost ORDER BY avg_cost limit 20").show()
        println("\nDo You Want To Save To A JSON File? [1] YES [2] NO")
        val save = readInt()
        save match{
          case 1 => {
            spark.sql("SELECT * FROM lowest_cost").write.json("output/lowest_cost.json")
            println("lowest_cost.json Saved")
          }
          case 2 => println("\nJSON File Not Saved")
          case _ => println("\nInvalid Selection")
        }
        println("\nDo You Want to Make More Query Selection? [1] YES [2] NO")
        val select = readInt()
        select match{
          case 1 => user_session()
          case 2 => user_page()
          case _ => println("\nInvalid Selection")
            user_session()
        }
      }
      case "5" => {
        println("\nEnter The State Initial You Want To Query: ")
        val state = readLine().toUpperCase
        spark.sql("DROP VIEW IF EXISTS fuel_type")
        spark.sql(s"CREATE VIEW IF NOT EXISTS fuel_type AS SELECT state, fuelType, COUNT(*) AS Count FROM rental GROUP BY state, fuelType")
        spark.sql(s"SELECT * FROM fuel_type WHERE state = '$state'").show()
        println("\nDo You Want To Save To A JSON File? [1] YES [2] NO")
        val save = readInt()
        save match{
          case 1 => {
            spark.sql("SELECT * FROM fuel_type").write.json("output/fuel_type.json")
            println("fuel_type.json Saved")
          }
          case 2 => println("\nJSON File Not Saved")
          case _ => println("\nInvalid Selection")
        }
        println("\nDo You Want to Make More Query Selection? [1] YES [2] NO")
        val select = readInt()
        select match{
          case 1 => user_session()
          case 2 => user_page()
          case _ => println("\nInvalid Selection")
            user_session()
        }
      }
      case "6" => {
        spark.sql("DROP VIEW IF EXISTS fuelTypeRate")
        spark.sql(s"CREATE VIEW IF NOT EXISTS fuelTypeRate AS SELECT fuelType, MEAN(dailyRate) AS Rate FROM rental GROUP BY fuelType")
        spark.sql("SELECT * FROM fuelTypeRate ORDER BY Rate DESC").show()
        println("\nDo You Want To Save To A JSON File? [1] YES [2] NO")
        val save = readInt()
        save match{
          case 1 => {
            spark.sql("SELECT * FROM fuelTypeRate").write.json("output/fuelTypeRate.json")
            println("fuelTypeRate.json Saved")
          }
          case 2 => println("\nJSON File Not Saved")
          case _ => println("\nInvalid Selection")
        }
        println("\nDo You Want to Make More Query Selection? [1] YES [2] NO")
        val select = readInt()
        select match{
          case 1 => user_session()
          case 2 => user_page()
          case _ => println("\nInvalid Selection")
            user_session()
        }
      }
      case "7" => {
        user_page()
      }
      case _ => println("\nInvalid Selection")
        user_session()
    }
  }

  def update_username(): Unit = {
    val spark = SparkSession
      .builder
      .appName("Car Rental Query")
      .master("local[*]")
      .config("spark.master", "local[*]")
      .config("spark.driver.allowMultipleContexts", "true")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    println("created spark session")
    val url = "jdbc:mysql://localhost:3306/p1"
    val user = "root"
    val pass = "N83ngmyun46$^"

    val sourceDf = spark.read.format("jdbc").option("url", url)
      .option("dbtable", "test1").option("user", user)
      .option("password", pass).load()


    sourceDf.createOrReplaceTempView("users1")

    println("Enter Current Username: ")
    val username = readLine()
    println("Enter New Username: ")
    val new_username = readLine()
    val x = spark.sql(s"SELECT * FROM users1").show()
    val y = spark.sql(s"UPDATE users1 SET username = '$new_username' WHERE username = '$username'").show()
    //val update_username = spark.sql(s"UPDATE users1 SET username = '$new_username' WHERE username = '$username'")
    //update_username.show()
/*    if (update_username.count() > 0) {
      println("Username Updated")
    } else {
      println("Username Not Updated")
    }*/
  }




}


