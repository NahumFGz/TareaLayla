// Databricks notebook source
dbfs:/FileStore/shared_uploads/20jomi36@gmail.com/persona.data
dbfs:/FileStore/shared_uploads/20jomi36@gmail.com/DATA_TRANSACCION.txt

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession

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

//Mostramos los datos
dfPersona.show()

//Cuando estamos desarrollando, el show nos ayudará a ver si el proceso está siendo implementado de manera correcta
//Pero recordemos que el SHOW al ser un action re-ejecuta toda la cadena de procesos asociada al dataframe

//Definimos una varible de control
var PARAM_SHOW_HABILITADO = true

//Importamos la librería que define el tipo de dato de un Dataframe
import org.apache.spark.sql.DataFrame

//Definimos la función
def show(df : DataFrame) = {
  if(PARAM_SHOW_HABILITADO == true){
    df.show()
  }
}

//PASO 1: Agrupamos los datos según la edad
var df1 = dfPersona.groupBy(dfPersona.col("EDAD")).agg(
	f.count(dfPersona.col("EDAD")).alias("CANTIDAD"), 
	f.min(dfPersona.col("FECHA_INGRESO")).alias("FECHA_CONTRATO_MAS_RECIENTE"), 
	f.sum(dfPersona.col("SALARIO")).alias("SUMA_SALARIOS"), 
	f.max(dfPersona.col("SALARIO")).alias("SALARIO_MAYOR")
)

//Mostramos los datos
show(df1)

//PASO 2: Filtramos por una EDAD
var df2 = df1.filter(df1.col("EDAD") > 35)

//Mostramos los datos
show(df2)

//PASO 3: Filtramos por SUMA_SALARIOS
var df3 = df2.filter(df2.col("SUMA_SALARIOS") > 20000)

//Mostramos los datos
show(df3)

//PASO 4: Filtramos por SALARIO_MAYOR
var dfResultado = df3.filter(df3.col("SALARIO_MAYOR") > 1000)

//Mostramos los datos
show(dfResultado)

//Escribimos el dataframe de la resultante final en disco duro
dfResultado.write.format("csv").mode("overwrite").option("header", "true").option("delimiter", "|").save("dbfs:///FileStore/_spark/output/dfResultado")
