package sonra.io.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scopt.OptionParser
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode


object TestSpark2 {

	def main(args: Array[String]) = {

			println("##Test##:Started\n")
			val parser = new scopt.OptionParser[Config]("SplitTsv") {
				head("SplitTsv", "Spark")

				opt[Unit]('r', "recursive").action ((_, c) =>
				c.copy(recursive = true)).text("The input path should be searched recursively for files (optional).")

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
			.appName("JavaALSExample")
			.config(conf)
			.config("spark.master", "local")
			.config("spark.sql.warehouse.dir", input)
			.getOrCreate();

			val readFileRDD = sp.sparkContext.textFile(input)
					readFileRDD.filter(x => x.startsWith("TOPUP") ).saveAsTextFile(outputTopout)
					readFileRDD.filter(x => x.startsWith("USAGE") ).saveAsTextFile(outputUsage)


					val dfTags = sp.read.textFile(input)
					dfTags.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(config.output+"/csv")
					dfTags.write.mode(SaveMode.Overwrite).json(config.output+"/json")
					dfTags.write.mode(SaveMode.Overwrite).parquet(config.output+"/parquet")
					dfTags.show(10) 
		

					sp.stop
					println("##Test##:Finished")
			} getOrElse {
	

			}

	}

case class Config(recursive: Boolean = true, input: String = null,
output:String = null)

}
