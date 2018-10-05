package sonra.io.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scopt.OptionParser
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._

object TestSpark4 {

	System.setProperty("hadoop.home.dir",  sys.env("HADOOP_HOME"));

	def main(args: Array[String]) = {

			println("## Test##:Started\n")
			val parser = new scopt.OptionParser[Config]("SplitTsv") {
				head("SplitTsv", "Spark")

				opt[String]('i', "input").required().action ( (x, c) =>
				c.copy(input = x)).text("input is the input path. Required.")

				opt[String]('o', "output") required() action { (x, c) =>
				c.copy(output = x) } text("output is the output path. Required.")
			}


			parser.parse(args, Config()) map { config =>

			val input = config.input
			val outputUsage = config.output +"/usage"
			val outputTopout = config.output +"/topout"

			println("Input path==" + input)
			println("Output path=" + config.output)

			//Start the Spark context
			val conf = new SparkConf()
			.setAppName("ReadTsv")
			.setMaster("local")
			.set("spark.hadoop.validateOutputSpecs", "false")  

			val sp = SparkSession
			.builder()
			.appName("SparkExample")
			.config(conf)
			.config("spark.master", "local")
			.config("spark.sql.warehouse.dir", input)
			.getOrCreate();

			import sp.implicits._

			val schema = StructType( 
					StructField("a", StringType, true) ::
						StructField("b", StringType, true):: 
							StructField("c", StringType, true):: 
								StructField("d", StringType, true):: 
									StructField("e", StringType, true):: 
										StructField("f", StringType, true) :: Nil)

			val readFile = sp.read.option("delimiter", " ").schema(schema).csv(input)
			val a= readFile.filter($"a".rlike("USAGE"))
			val b= readFile.filter($"a".rlike("TOPUP"))
			//a.rdd.saveAsTextFile(outputUsage)
			//b.rdd.saveAsTextFile(outputTopout)
			
			readFile.show()
			readFile.printSchema()

			a.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("delimiter", " ").save(outputUsage)
			b.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("delimiter", " ").save(outputTopout)



			
			a.write.mode(SaveMode.Overwrite).json(outputUsage+"/json")
			b.write.mode(SaveMode.Overwrite).json(outputTopout+"/json")

			a.write.mode(SaveMode.Overwrite).parquet(outputUsage+"/parquet")
			b.write.mode(SaveMode.Overwrite).parquet(outputTopout+"/parquet") 

			//Using this format saves partitioned
			//val read = sp.sparkContext.textFile(input)
			//		read.filter(x => x.startsWith("TOPUP") ).saveAsTextFile(outputTopout)
			//		read.filter(x => x.startsWith("USAGE") ).saveAsTextFile(outputUsage)

			sp.stop
			println("## Test##:Finished")
			} getOrElse {


			}
	}

case class Config(input: String = null, output:String = null)

}
