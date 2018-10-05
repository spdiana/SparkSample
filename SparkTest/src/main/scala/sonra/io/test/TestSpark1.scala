package sonra.io.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scopt.OptionParser
import org.apache.spark.sql.SQLContext


object TestSpark1 {

	def main(args: Array[String]) = {

			System.setProperty("hadoop.home.dir",  sys.env("HADOOP_HOME"));

			println("##Sonra Test##:Started\n")
			val parser = new scopt.OptionParser[Config]("SplitTsv") {
				head("SplitTsv", "Spark")

				opt[String]('i', "input").required().action ( (x, c) =>
				c.copy(input = x)).text("The input path. Required.")

				opt[String]('o', "output") required() action { (x, c) =>
				c.copy(output = x) } text("The output path. Required.")
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

					val sc = new SparkContext(conf)
					val rdd = sc.textFile(input)

					rdd.filter(x => x.startsWith("TOPUP") ).saveAsTextFile(outputTopout)
					rdd.filter(x => x.startsWith("USAGE") ).saveAsTextFile(outputUsage)

					sc.stop
					println("##Sonra Test##:Finished")
			} getOrElse {

			}

	}

case class Config(input: String = null, output:String = null)

}