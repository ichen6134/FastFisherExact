// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC Fast Fisher's Exact Test (by Isabelle Chen)
// MAGIC 
// MAGIC <H3>This notebook does the following calculations</H3>
// MAGIC 
// MAGIC 
// MAGIC - Fisher's Exact Test with a single 2x2 contingency table
// MAGIC   - No distribution
// MAGIC   - Distribution with different number of partitions / parallelisms
// MAGIC - Batch of Fisher's Exact Tests with 100000 2x2 contingency tables
// MAGIC   - No distribution
// MAGIC   - Distribution with different number of partitions / parallelisms

// COMMAND ----------

// DBTITLE 1,Define the functions for calculating hypergeometric distribution and Fisher's Exact Test p-values
import org.apache.spark.sql.functions._

// n choose k
def nck(n: Int, k: Int): Double = {
  if (k == 0) {
    1
  } else {
    var result: Double = 1.0
    for (i <-  1 to k ) {
      result = result * (n - k + i)
      result = result / i
    }
    result
  }
}

// hypergeometric distribution p-value
def calHyperGeometricDistribution(a: Int, b: Int, c: Int, d: Int): Double = {
  val row1Sum = a + b
  val row2Sum = c + d
  val total = a + b + c + d
  /*
  
              [(a+b).chose(a)] * [(c+d).chose(c)]
     pValue = -------------------------------------
                   [(a+b+c+d).chose(a+c)]
 */
  
  val pValue = 1.0 * nck(row1Sum, a) * nck (row2Sum, c) / nck(total, (a + c))
  pValue
}

// User defined function for calculating hypergeometric distribution p-value
val calHyperGeometricDistributionUdf = udf(calHyperGeometricDistribution(_: Int, _:Int, _:Int, _: Int))


// Calculate the sum of p-values from hypergeometric distributions
// This method can be used to calculate the Fisher's Exact Test p-value.
def calHyperGeometricDistributionSum(start: Int, end: Int): Double = {
  val a = 199
  val b = 101
  val c = 701
  val d = 199
  val row1Sum = a + b
  val row2Sum = c + d
  val col1Sum = a + c
  val col2Sum = b + d
  val total = a + b +c + d 

  val currPvalue = calHyperGeometricDistribution(a, b, c, d)
  var pValue = 0.0
  var startTime = System.currentTimeMillis
  for (newA <- (start to end)) {
    val newC = col1Sum - newA
    val newB = row1Sum - newA
    val newD = col2Sum - newB
    val newPvalue = calHyperGeometricDistribution(newA, newB, newC, newD)
    if (newPvalue <= currPvalue) {
      pValue = pValue + newPvalue
    }
  }
  pValue
}

// User defined function for calculating the sum of p-values from hypergeometric distributions
val calHyperGeometricDistributionSumUdf = udf(calHyperGeometricDistributionSum(_: Int, _: Int))

// COMMAND ----------

val a = 199
val b = 101
val c = 701
val d = 199
val row1Sum = a + b
val row2Sum = c + d
val col1Sum = a + c
val col2Sum = b + d
val total = a + b +c + d 

val currPvalue = calHyperGeometricDistribution(a, b, c, d)
var pValue = 0.0
var startTime = System.currentTimeMillis
for ( i <- 1 to 100000 ) {
  for (newA <- (0 to a)) {
    val newC = col1Sum - newA
    val newB = row1Sum - newA
    val newD = col2Sum - newB
    val newPvalue = calHyperGeometricDistribution(newA, newB, newC, newD)
    if (newPvalue <= currPvalue) {
      pValue = pValue + newPvalue
    }
  }
}
val endTime = System.currentTimeMillis

val avgRuntime = (endTime - startTime)

pValue / 100000.0


// COMMAND ----------

// DBTITLE 1,Create the dataframe with the 2x2 contingency table values
import scala.collection.mutable.ListBuffer

val a = 199
val b = 101
val c = 701
val d = 199
val row1Sum = a + b
val row2Sum = c + d
val col1Sum = a + c
val col2Sum = b + d
val total = a + b +c + d 
val currPvalue = calHyperGeometricDistribution(a, b, c, d)

val listBuff: ListBuffer[(Int, Int, Int, Int)] = new ListBuffer()

var startTime = System.currentTimeMillis

for (newA <- (0 to a)) {
  val newC = col1Sum - newA
  val newB = row1Sum - newA
  val newD = col2Sum - newB
    
  listBuff += ((newA, newB, newC, newD))
}

val dataList = listBuff.toList
val df = listBuff.toList.toDF("a", "b", "c", "d")

df.cache
df.count

// COMMAND ----------

spark.conf.set("spark.default.parallelism", 200)
df.withColumn("fisher_exact_p_value", calHyperGeometricDistributionUdf($"a", $"b", $"c", $"d")).filter(s"fisher_exact_p_value <= $currPvalue").select("fisher_exact_p_value").as[Double].collect.sum

// COMMAND ----------

for (i <- 1 to 10) {
 calHyperGeometricDistributionSum(0, 199)
}

// COMMAND ----------

// DBTITLE 1,Create dataframe to run multiple steps of Fisher's Exact Test in one partitions
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.mutable.ListBuffer

def createDF(step: Int): DataFrame = {
  val listBuff: ListBuffer[(Int, Int)] = new ListBuffer()
  val seq = (0 to 199 by step).toList

  for (i <- 0 to (seq.length - 2)) {
    listBuff += ((seq(i), seq(i + 1)))
  }

  val df = listBuff.toList.toDF("start", "end")
  
  val newDF = df.repartition(200/step)
  newDF.cache
  newDF.count
  newDF
}



// COMMAND ----------

// DBTITLE 1,step = 1, number of partitions = 200
val df1 = createDF(1)


// COMMAND ----------

// DBTITLE 1,Run the calculation 10 times
for (i <- 1 to 10) {
  df1.withColumn("pValue", calHyperGeometricDistributionSumUdf($"start", $"end")).select("pValue").as[Double].collect().sum
}

// COMMAND ----------

// DBTITLE 1,step = 5, number of partitions = 40
val df5 = createDF(5)

// COMMAND ----------

for (i <- 1 to 10) {
  df5.withColumn("pValue", calHyperGeometricDistributionSumUdf($"start", $"end")).select("pValue").as[Double].collect().sum
}

// COMMAND ----------

// DBTITLE 1,step = 10, number of partitions = 20
val df10 = createDF(10)

// COMMAND ----------

for (i <- 1 to 10) {
  df10.withColumn("pValue", calHyperGeometricDistributionSumUdf($"start", $"end")).select("pValue").as[Double].collect().sum
}

// COMMAND ----------

// DBTITLE 1,step = 20, number of partitions = 10
val df20 = createDF(20)

// COMMAND ----------

for (i <- 1 to 10) {
  df20.withColumn("pValue", calHyperGeometricDistributionSumUdf($"start", $"end")).select("pValue").as[Double].collect().sum
}

// COMMAND ----------

// DBTITLE 1,step = 50, number of partitions = 4
val df50 = createDF(50)

// COMMAND ----------

for (i <- 1 to 10) {
  df50.withColumn("pValue", calHyperGeometricDistributionSumUdf($"start", $"end")).select("pValue").as[Double].collect().sum
}

// COMMAND ----------

// DBTITLE 1,step = 100,  number of partitions = 2
val df100 = createDF(100)

// COMMAND ----------

for (i <- 1 to 10) {
  df100.withColumn("pValue", calHyperGeometricDistributionSumUdf($"start", $"end")).select("pValue").as[Double].collect().sum
}

// COMMAND ----------

// DBTITLE 1,step = 200, number of partitions = 1
val df200 = createDF(200)

// COMMAND ----------

for (i <- 1 to 10) {
  df200.withColumn("pValue", calHyperGeometricDistributionSumUdf($"start", $"end")).select("pValue").as[Double].collect().sum
}

// COMMAND ----------

// DBTITLE 1,Create a dataframe to run multiple Fisher's Exact Tests in one partition
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

def createDF2(numOfTests: Int, numPartitions: Int): DataFrame = {
  val df = (0 to numOfTests -1 ).toList.toDF("id")
  val newDF = df.repartition(numPartitions)
  newDF.cache
  newDF.count
  newDF
}



// COMMAND ----------

// DBTITLE 1,number of partitions = 1,  number of Fisher's Exact Tests = 100000
val df1 = createDF2(100000, 1)

// COMMAND ----------

for (i < 1 to 3) {
  df1.withColumn("pValue", calHyperGeometricDistributionSumUdf(lit(0), lit(199))).select("pValue").as[Double].collect().sum / 100000.0
}

// COMMAND ----------

// DBTITLE 1,number of partitions = 5,  number of Fisher's Exact Tests = 100000
val df5 = createDF2(100000, 5)

// COMMAND ----------

for (i < 1 to 3) {
  df5.withColumn("pValue", calHyperGeometricDistributionSumUdf(lit(0), lit(200))).select("pValue").as[Double].collect().sum / 100000.0
}

// COMMAND ----------

// DBTITLE 1,number of partitions = 10,  number of Fisher's Exact Tests = 100000
val df10 = createDF2(100000, 10)

// COMMAND ----------

for (i < 1 to 3) {
  df10.withColumn("pValue", calHyperGeometricDistributionSumUdf(lit(0), lit(200))).select("pValue").as[Double].collect().sum / 100000.0
}

// COMMAND ----------

// DBTITLE 1,number of partitions = 20,  number of Fisher's Exact Tests = 100000
val df20 = createDF2(100000, 20)

// COMMAND ----------

for (i < 1 to 3) {
  df20.withColumn("pValue", calHyperGeometricDistributionSumUdf(lit(0), lit(200))).select("pValue").as[Double].collect().sum / 100000.0
}

// COMMAND ----------

// DBTITLE 1,number of partitions = 50,  number of Fisher's Exact Tests = 100000
val df50 = createDF2(100000, 50)

// COMMAND ----------

for (i < 1 to 3) {
  df50.withColumn("pValue", calHyperGeometricDistributionSumUdf(lit(0), lit(200))).select("pValue").as[Double].collect().sum / 100000.0
}

// COMMAND ----------

// DBTITLE 1,number of partitions = 100,  number of Fisher's Exact Tests = 100000
val df100 = createDF2(100000, 100)

// COMMAND ----------

for (i < 1 to 3) {
  df100.withColumn("pValue", calHyperGeometricDistributionSumUdf(lit(0), lit(200))).select("pValue").as[Double].collect().sum / 100000.0
}

// COMMAND ----------

// DBTITLE 1,number of partitions = 200,  number of Fisher's Exact Tests = 100000
val df200 = createDF2(100000, 200)

// COMMAND ----------

for (i < 1 to 3) {
  df200.withColumn("pValue", calHyperGeometricDistributionSumUdf(lit(0), lit(200))).select("pValue").as[Double].collect().sum / 100000.0
}

// COMMAND ----------

// DBTITLE 1,Fisher's Exact Tests = 100000, no distribution
for (i <-0 to 99999) {
  calHyperGeometricDistributionSum(0, 199)
}
