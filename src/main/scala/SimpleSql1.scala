/* SimpleSql1.scala */
import org.apache.spark.sql.SparkSession
import java.io.File
import java.io.PrintWriter
// import spark.implicits._

object SimpleSql1 {
  def main(args: Array[String]) {
    
      // Added folowing statement to get rid of userlogin error caused while running the code from IDE on dev/linux box
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("vy0769"))
    
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()

    import spark.implicits._

    val df = spark.read.json("/home/vijay/Downloads/spark-2.2.0-bin-hadoop2.7/examples/src/main/resources/people.json")

    df.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // Select only the "name" column
    df.select("name").show()
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     null|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+

    // Select people older than 21
    df.filter($"age" > 21).show()
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+

    // Count people by age
    df.groupBy("age").count().show()
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+


    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

  }
}
