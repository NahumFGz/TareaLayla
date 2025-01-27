// Databricks notebook source
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

//Leemos el archivo de persona
var dfPersona = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        Array(
            StructField("ID", StringType, true),
            StructField("NOMBRE", StringType, true),
            StructField("TELEFONO", StringType, true),
            StructField("CORREO", StringType, true),
            StructField("FECHA_INGRESO", StringType, true),
            StructField("EDAD", IntegerType, true),
            StructField("SALARIO", DoubleType, true),
            StructField("ID_EMPRESA", StringType, true)
        )
    )
).load("dbfs:/FileStore/shared_uploads/20jomi36@gmail.com/persona.data")

dfPersona.show()

//Leemos las transacciones
var dfTransaccion = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(
    StructType(
        Array(
            StructField("ID_PERSONA", StringType, true),
            StructField("ID_EMPRESA", StringType, true),
            StructField("MONTO", DoubleType, true),
            StructField("FECHA", StringType, true)
        )
    )
).load("dbfs:/FileStore/shared_uploads/20jomi36@gmail.com/DATA_TRANSACCION.txt")

//Mostramos los datos
dfTransaccion.show()

//Seleccionamos las personas con más de 30 años
var df1 = dfPersona.filter(dfPersona.col("EDAD") > 30)

//Seleccionamos las transacciones con más de 1000 dólares
var df2 = dfTransaccion.filter(dfTransaccion.col("MONTO") > 1000)

//Cruzamos las transacciones con los datos de las personas
var df3 = df2.join(
  df1, 
  df2.col("ID_PERSONA") === df1.col("ID"),
  "inner"
).select(
  df1.col("NOMBRE"), 
  df1.col("EDAD"), 
  df1.col("SALARIO"), 
  df2.col("MONTO"), 
  df2.col("FECHA")
)

//Filtramos las transacciones que sean del día "2021-01-23"
var df4 = df3.filter(df3.col("FECHA") === "2021-01-23")

//Mostramos los datos
df4.show()

//Implementacion de la funcion
def cache(df : DataFrame) = {
  print("Almacenando en cache...")
  df.cache()
  println(", almacenado en cache!")
}

// COMMAND ----------

//Ejemplo de uso
cache(df4)

// COMMAND ----------

//FUNCION PARA LIBERAR DEL CACHÉ UN DATAFRAME
def liberarCache(df : DataFrame) = {
  print("Liberando cache...")
  df.unpersist(blocking = true)
  println(", cache liberado!")
}

liberarCache(df4)

