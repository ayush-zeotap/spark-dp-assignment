import com.databricks.spark.avro._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

object Driver {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Assignment")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val schemaText = spark.sparkContext.wholeTextFiles("./src/main/resources/outputSchema.avsc").collect()(0)._2
    val inputPath_4 = "src/main/resources/part-00000-a4794630-9584-4152-b5ed-595fd7608322.avro"
    val inputPath_212 = "src/main/resources/part-00000-64940e90-dcc3-4d23-9a38-c7e1e1d24820.avro"
    val inputPath_316 = "src/main/resources/part-00000-a3521293-47b8-4b2c-983a-2b973319fd51.avro"

    val inputDf_4 = spark.read.option("avroSchema", schemaText).avro(inputPath_4)
    val inputDf_212 = spark.read.option("avroSchema", schemaText).avro(inputPath_212)
    val inputDf_316 = spark.read.option("avroSchema", schemaText).avro(inputPath_316)

    val priority_matrix = spark.read.json("src/main/scala/priority.json")


    def features_order(data_feature: String) = {
      val target_feature = priority_matrix.select(data_feature).head().getList(0).toArray
      var priority = Seq[Column]()
      for (dp <- target_feature) {
        if (dp == 4) priority = priority :+ inputDf_4(data_feature)
        else if (dp == 212) priority = priority :+ inputDf_212(data_feature)
        else priority = priority :+ inputDf_316(data_feature)
      }
      priority
    }

    val merged_final = inputDf_4.join(inputDf_212, Seq("Identifier")).join(inputDf_316, Seq("Identifier"))
      .select(inputDf_4("Identifier"), coalesce(features_order("Age"): _*).as("Age"),
        coalesce(features_order("Gender"): _*).as("Gender"),
        coalesce(features_order("Zipcode"): _*).as("Zipcode"),
        coalesce(features_order("Device"): _*).as("Device"),
        coalesce(features_order("Language"): _*).as("Language"))
    val overlap = inputDf_4.join(inputDf_316, "Identifier")

    val distinct_id = merged_final.count()

    val dp4_age = merged_final.filter(merged_final("DpId") === 4 && merged_final("Age").isNotNull).count()
    val dp212_age = merged_final.filter(merged_final("DpId") === 212 && merged_final("Age").isNotNull).count()
    val dp316_age = merged_final.filter(merged_final("DpId") === 316 && merged_final("Age").isNotNull).count()

    val dp4_gender = merged_final.filter(merged_final("DpId") === 4 && merged_final("Gender").isNotNull).count()
    val dp212_gender = merged_final.filter(merged_final("DpId") === 212 && merged_final("Gender").isNotNull).count()
    val dp316_gender = merged_final.filter(merged_final("DpId") === 316 && merged_final("Gender").isNotNull).count()

    val dp4_zipcode = merged_final.filter(merged_final("DpId") === 4 && merged_final("Zipcode").isNotNull).count()
    val dp212_zipcode = merged_final.filter(merged_final("DpId") === 212 && merged_final("Zipcode").isNotNull).count()
    val dp316_zipcode = merged_final.filter(merged_final("DpId") === 316 && merged_final("Zipcode").isNotNull).count()


    def age_bucket = udf((age: Int) =>
      if (age <= 18) 18
      else if (age > 18 && age <= 25) 25
      else if (age > 25 && age <= 35) 35
      else if (age > 35 && age <= 45) 45
      else if (age > 45 && age <= 55) 55
      else if (age > 55 && age <= 65) 65
      else 75
    )

    val final_df = merged_final.withColumn("Age", age_bucket(col("Age")))

    val age_count = final_df.groupBy("Age").count()
    val gender_count = final_df.groupBy("Gender").count()

  }
}
