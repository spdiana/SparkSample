package sonra.io.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scopt.OptionParser
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object TestSpark3 {

	def main(args: Array[String]) = {

			println("##Test##:Started\n")

			val usage = """  Use: <jar file name> <input path name> <output path name> """

			if (args.length < 2 ) {
				println(usage)
				System.exit(0);
			}

			val input = args(0)
					val output = args(1)

					val outputUsage = output +"/usage"
					val outputTopout = output +"/topout"

					println("Input path==" + input)
					println("Output path=" + output)

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
					dfTags.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(output+"/csv")
					dfTags.write.mode(SaveMode.Overwrite).json(output+"/json")
					dfTags.write.mode(SaveMode.Overwrite).parquet(output+"/parquet")
					dfTags.show(10)

					sp.stop
					println("##Test##:Finished")




	}

case class Config(recursive: Boolean = true, input: String = null,
output:String = null)

}
