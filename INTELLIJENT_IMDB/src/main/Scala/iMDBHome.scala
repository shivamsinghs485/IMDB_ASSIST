import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StringType}
import org.apache.spark.sql.functions.{min, max}
import org.apache.spark.sql.functions._


object iMDBHome extends App {
	
	val spark=SparkSession.builder().appName("iMDBHome").config("spark.master","local").getOrCreate()
	
	val custSchema=StructType(
		Array(
			StructField("nconst" , StringType),
			StructField("primaryName",StringType),
			StructField("birthYear",DateType),
			StructField("deathYear",DateType),
			StructField("primaryProfession",StringType),
			StructField("knownForTitles",StringType)
		
		)
	) //Static Schema
	
	val titleBasics = spark.read.format("CSV")
		.options(Map("inferSchema" -> "true","header" -> "true","Sep" -> "\t","dateFormat" -> "y")).load("C:/Users/Jarvis/Desktop/IMDB/title.basics.tsv/data.tsv")
	//val titleratings = spark.read.format("CSV")
	//	.options(Map("inferSchema" -> "true","header" -> "true","Sep" -> "\t","dateFormat" -> "y")).load("C:/Users/Jarvis/Desktop/IMDB/title.ratings.tsv/data.tsv")
	//val knownFor = spark.read.format("CSV")
	//	.options(Map("inferSchema" -> "true","header" -> "true","Sep" -> "\t","dateFormat" -> "y")).load("C:/Users/Jarvis/Desktop/IMDB/title.akas.tsv/data.tsv")
	//val principals = spark.read.format("CSV")
	//	.options(Map("inferSchema" -> "true","header" -> "true","Sep" -> "\t","dateFormat" -> "y")).load("src/main/resources/IMDB/title.principals.tsv/data.tsv")
	
	
	//val popularMovie =	new PopularMovie()
	//val a=popularMovie.usingGroupBy(titleBasics,titleratings).rdd
	//a.take(10).foreach(println)
	titleBasics.rdd.repartition(3)
	println(titleBasics.count())
	println(titleBasics.rdd.partitions.size)
	//println(titleratings.rdd.partitions.size)
	//titleBasics.show(10)
	//titleratings.show(10)
	
	System.in.read() //Just to hold Context From getting auto Closed
	
	
}
