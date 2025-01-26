// Databricks notebook source
// MAGIC %md
// MAGIC CONFIGURAR LA SESIÓN SPARK

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

//Reserva de recursos
var spark = SparkSession.builder.
appName("Mi Aplicacion").
config("spark.driver.memory", "1000g"). // memoria maxima del cluster
config("spark.dynamicAllocation.maxExecutors", "100"). //numero maximo de ejecutores dinamicos
config("spark.executor.cores", "4"). //cantidad de nucleos
config("spark.executor.memory", "5g"). //memoria asignada a cada ejecutor
config("spark.executor.memoryOverhead", "100g"). //memoria de reserva
config("spark.default.parallelism", "100"). //trabajos en paralelo que se pueden realizar
getOrCreate()


// COMMAND ----------

// MAGIC %md
// MAGIC PARTE 1

// COMMAND ----------

//Leemos el archivo de persona
var dfRiesgo = spark.read.format("csv").option("header", "true").option("delimiter", ";").schema(
    StructType(
        Array(
            StructField("ID_CLIENTE", StringType, true),
            StructField("RIESGO_CENTRAL_1", DoubleType, true),
            StructField("RIESGO_CENTRAL_2", DoubleType, true),
            StructField("RIESGO_CENTRAL_3", DoubleType, true)
        )
    )
).load("/FileStore/tables/RIESGO_CREDITICIO.csv")

// COMMAND ----------

val jsonDF: DataFrame = spark.read
  .option("inferSchema", "true") // Inferir el esquema automáticamente
  .json("/FileStore/tables/transacciones_bancarias.json")

jsonDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC PARTE 2

// COMMAND ----------

import org.apache.spark.sql.functions.col

val dfTransaccion = 
jsonDF.select(
  col("EMPRESA.ID_EMPRESA").alias("ID_EMPRESA"),
  col("PERSONA.ID_PERSONA").alias("ID_PERSONA"),
  col("TRANSACCION.MONTO").alias("MONTO"),
  col("TRANSACCION.FECHA").alias("FECHA")
)

val dfEmpresa = 
jsonDF.select(
  col("EMPRESA.ID_EMPRESA").alias("ID_EMPRESA"),
  col("EMPRESA.NOMBRE_EMPRESA").alias("NOMBRE_EMPRESA")
)

val dfPersona = 
jsonDF.select(
  col("PERSONA.ID_PERSONA").alias("ID_PERSONA"),
  col("PERSONA.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
  col("PERSONA.EDAD").alias("EDAD"),
  col("PERSONA.SALARIO").alias("SALARIO"),
)


// COMMAND ----------

// MAGIC %md
// MAGIC PARTE 3

// COMMAND ----------

//Limpieza e la tabla persona
val filteredDfPersona = dfPersona.filter($"EDAD".between(0, 60) && $"SALARIO".between(0, 1000000) && $"ID_PERSONA".isNotNull)
filteredDfPersona.show()

//Limpieza e la tabla empresa
val filteredDfEmpresa = dfEmpresa.filter($"ID_EMPRESA".isNotNull)
filteredDfEmpresa.show()

//Limpieza e la tabla transaccion
val filteredDfTransaccion = dfTransaccion.filter($"ID_PERSONA".isNotNull && $"MONTO".between(0, 1000000) && $"ID_PERSONA".isNotNull)
filteredDfTransaccion.show()

//Limpieza e la tabla riesgo
val filteredDfRiesgo = dfRiesgo.filter($"ID_CLIENTE".isNotNull && $"RIESGO_CENTRAL_1".between(0,1) && $"RIESGO_CENTRAL_2".between(0,1) && $"RIESGO_CENTRAL_3".between(0,1))
filteredDfRiesgo.show()  


// COMMAND ----------

// MAGIC %md
// MAGIC PARTE 4

// COMMAND ----------

val calcularRiesgoPonderado = udf((riesgo1: Double, riesgo2: Double, riesgo3: Double) => {
  (2 * riesgo1 + 3 * riesgo2 + 3 * riesgo3) / 7
})

val dfRiesgoPonderado = dfRiesgo.withColumn("riesgo_ponderado", calcularRiesgoPonderado(
  col("RIESGO_CENTRAL_1"),
  col("RIESGO_CENTRAL_2"),
  col("RIESGO_CENTRAL_3")
))

dfRiesgoPonderado.show()

// COMMAND ----------

// MAGIC %md
// MAGIC PARTE 5

// COMMAND ----------

//Union transacciones y persona
val filteredDfTransaccion2 = filteredDfTransaccion
  .withColumnRenamed("ID_PERSONA", "ID_PERSONAx")

val df1 = filteredDfTransaccion2.join(filteredDfPersona, filteredDfTransaccion2("ID_PERSONAx") === filteredDfPersona("ID_PERSONA"), "inner").dropDuplicates

//Union df1 y empresa
val filteredDfEmpresa2 = filteredDfEmpresa
  .withColumnRenamed("ID_EMPRESA", "ID_EMPRESAx")

val dfTablon = df1.join(filteredDfEmpresa2, df1("ID_EMPRESA") === filteredDfEmpresa2("ID_EMPRESAx"), "inner").dropDuplicates

//Eliminar columnas innecesarias
val columnasNecesarias = Seq("ID_EMPRESA","ID_PERSONA","MONTO","FECHA","NOMBRE_PERSONA","EDAD","SALARIO","NOMBRE_EMPRESA")

val dfTablon2 = dfTablon.select(columnasNecesarias.head, columnasNecesarias.tail: _*)

//Union dfTablon y dfRiesgos
val df2 = dfTablon2.join(dfRiesgoPonderado, dfTablon2("ID_PERSONA") === dfRiesgoPonderado("ID_CLIENTE"), "inner").dropDuplicates

//Visualizar
df1.show()
dfTablon.show()
df2.show()

// COMMAND ----------

// MAGIC %md
// MAGIC PARTE 6

// COMMAND ----------

//PRE PROCESAMIENTO

val dfTablon1 = df2.filter($"MONTO" > 500 && $"NOMBRE_EMPRESA" === "Amazon")
dfTablon1.show()

// COMMAND ----------

// MAGIC %md
// MAGIC PARTE 7

// COMMAND ----------

//PROCESAMIENTO

val dfReporte1 = dfTablon1.filter($"EDAD_PERSONA".between(30,39) && $"SALARIO".between(1000,5000))

val dfReporte2 = dfTablon1.filter($"EDAD_PERSONA".between(40,49) && $"SALARIO".between(2500,7000))

val dfReporte3 = dfTablon1.filter($"EDAD_PERSONA".between(50,60) && $"SALARIO".between(3500,10000))

// COMMAND ----------

// MAGIC %md
// MAGIC PARTE 8

// COMMAND ----------

//ALMACENAMIENTO
//REPORTE 1
dfReporte1.write
.format("csv")                        // Especificar formato CSV
.option("header", "true")             // Activar la cabecera
.option("delimiter", "|")             // Delimitador personalizado
.option("encoding", "ISO-8859-1")     // Configurar encoding
.mode("overwrite")                    // Modo de escritura (overwrite, append, etc.)
.save("dbfs:///FileStore/_bigdata/output/arquetipo-proc-avanzado/REPORTE_1") // Especificar ruta de destino

//REPORTE 2
dfReporte2.write
.format("csv")                        // Especificar formato CSV
.option("header", "true")             // Activar la cabecera
.option("delimiter", "|")             // Delimitador personalizado
.option("encoding", "ISO-8859-1")     // Configurar encoding
.mode("overwrite")                    // Modo de escritura (overwrite, append, etc.)
.save("dbfs:///FileStore/_bigdata/output/arquetipo-proc-avanzado/REPORTE_2") // Especificar ruta de destino

//REPORTE 3
dfReporte2.write
.format("csv")                        // Especificar formato CSV
.option("header", "true")             // Activar la cabecera
.option("delimiter", "|")             // Delimitador personalizado
.option("encoding", "ISO-8859-1")     // Configurar encoding
.mode("overwrite")                    // Modo de escritura (overwrite, append, etc.)
.save("dbfs:///FileStore/_bigdata/output/arquetipo-proc-avanzado/REPORTE_3") // Especificar ruta de destino
