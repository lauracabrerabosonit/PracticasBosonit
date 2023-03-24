### 1. Dataset

Los datos de origen son proporcionados en un archivos csv:

* udfs: dataset con datos de operaciones financieras.

### 2. Columnas y significado:

* nb: número de referencia de la operación.
* contract: identificador de contrato.
* udf_ref: identificador de operación de trading.
* fmly: familia a la que pertenece la operación financiera.
* grp: grupo al que pertenece la operación financiera.
* type: tipo de operación financiera.
* country: país de origen de la operación.
* udf_name: campo informado en el registro.
* num_value: valor numérico.
* string_value: valor de cadena de caracteres.
* date_value: valor de fecha.
* data_timestamp_part: marca temporal.
* data_date_part: fecha en la que se almacena la información.
* source_system: fuente de los datos.

### 3. Descripción del problema:

Si hacemos una visión general a nuestro conjunto de datos, podemos observar como hay hasta 10 registros (filas) para cada valor de *nb*, donde cada registro solo da información para un valor de *udf_name*. Esto es un gasto innecesario de almacenamiento y computación, además de complicar los futuros cálculos derivados de estos datos. Por esta razón, necesitamos convertir estos registros con el mismo *nb* a un solo registro.

Nuestro dataframe final tendrá que contener las siguientes columnas: `nb, M_CCY, M_CLIENT, M_CRDTCHRG, M_DIRECTIAV, M_DISCMARGIN, M_LIQDTYCHRG, M_MVA, M_RVA, M_SELLER, M_SUCURSAL`

* nb: debe contener el número de referencia de la operación.
* M_CLIENT, M_SELLER, M_CCY, M_SUCURSAL: deben mapear el valor de *string_value*
* M_DISCMARGIN, M_DIRECTIAV, M_LIQDTYCHRG, M_CRDTCHRG, , M_MVA, M_RVA: deben mapear el valor de *num_value*


Una vez tengamos este resultado, necesitaremos eliminar las operaciones que no tengan informados ninguno de los siguientes campos:

M_DISCMARGIN, M_DIRECTIAV, M_LIQDTYCHRG, M_CRDTCHRG, M_MVA, M_RVA, M_SELLER

No informados en este caso significa que o son valores nulos, vacíos o 0, en el caso de los campos numéricos.

### 4. Reto:

* Obtener un dataframe final que contenga las columnas indicadas, con un registro por *nb* y con los valores correctos mapeados.
* Las operaciones con los campos M_DISCMARGIN, M_DIRECTIAV, M_LIQDTYCHRG, M_CRDTCHRG, , M_MVA, M_RVA, M_SELLER no informados no deben existir.
* Hacerlo de la manera más eficiente posible a nivel computacional.

**NOTA:** Cada uno de los pasos descritos en el problema pueden efectuarse en una sola línea.