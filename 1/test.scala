import spark.implicits._
val columns = Seq("language","users_count")
val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
var dfFromData2 = spark.createDataFrame(data).toDF(columns:_*)
dfFromData2.show()
