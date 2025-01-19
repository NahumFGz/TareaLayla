Supongamos que tienes una base de datos con la siguiente tabla llamada employees:
from pyspark.sql import SparkSession

spark = SparkSession.builder \
 .appName("Spark SQL Exercises") \
 .getOrCreate()

data = [
("John Doe", "Engineering", 80000, "2023-01-15"),
("Jane Smith", "Marketing", 95000, "2022-11-23"),
("Alice Johnson", "Engineering", 85000, "2021-03-08"),
("Bob Brown", "Sales", 72000, "2024-02-25"),
("Charlie Davis", "Marketing", 90000, "2023-05-19")
]

columns = ["name", "department", "salary", "hire_date"]

df = spark.createDataFrame(data, columns)
df.createOrReplaceTempView("employees")

- Ejercicio 1: Consultar Todos los Empleados
  Consulta: Selecciona todos los empleados de la tabla employees.
- Ejercicio 2: Filtrar Empleados por Departamento
  Consulta: Encuentra todos los empleados en el departamento de "Marketing".
- Ejercicio 3: Ordenar Empleados por Salario
  Consulta: Ordena a todos los empleados por salario en orden descendente.
- Ejercicio 4: Calcular el Salario Promedio por Departamento
  Consulta: Calcula el salario promedio de los empleados en cada departamento.
- Ejercicio 5: Encontrar Empleados Contratados Después de una Fecha
  Consulta: Encuentra todos los empleados que fueron contratados después del 1 de enero de 2023.
