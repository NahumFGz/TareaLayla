Los datos adjuntos a esta lección forman parte de la base de datos [NeurIPS 2020] Data Science for COVID-19 (DS4C) disponible en Kaggle. Estos datos hacen referencia a los casos de contagio de covid-19 en Corea del Sur.

El archivo Case.csv contiene los casos reportados y el archivo csv PatientInfo contiene la información de los pacientes.

1. A partir del archivo csv Case, determine las tres ciudades con más casos confirmados de la enfermedad. La salida debe contener tres columnas: provincia, ciudad y casos confirmados. El resultado debe contener exactamente los tres nombres de ciudades con más casos confirmados ya que no se admiten otros valores.

2. Cree un dataframe a partir del archivo csv PatientInfo. Asegúrese de que su dataframe no contenga pacientes duplicados.

- ¿Cuántos pacientes tienen informado por quién se contagiaron(columna infected_by)? Obtenga solo los pacientes que tengan informado por quién se contagió.
- A partir de la salida del inciso anterior obtenga solo los pacientes femeninos. La salida no debe contener las columnas released_date y deceased_date.
- Establezca el número de particiones del dataframe resultante del inciso anterior en dos. Escriba el dataframe resultante en un archivo parquet, la salida debe estar particionada por la provincia y el modo de escritura debe ser overwrite.
