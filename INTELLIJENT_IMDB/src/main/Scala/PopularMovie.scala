import iMDBHome.{titleBasics, titleratings}
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, types}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.functions._
class PopularMovie {
	def usingGroupBy(titleBasics: DataFrame , titleratings : DataFrame): DataFrame =
	{
		val condition = titleBasics.col("tconst") === titleratings.col("tconst")
		return titleBasics.select("primaryTitle","tconst" ,"genres").join(titleratings.select("numVotes","tconst") ,condition ,"inner")
			.groupBy("genres","primaryTitle").max("numVotes").sort(desc("max(numVotes)"))
	}
	
}
