### 1. Datasets

Los datos de origen constan de dos archivos csv con la misma estructura y tipo de columnas.

* trade_details: dataset original con datos reales de operaciones financieras.
* trade_details_snapshot: copia de seguridad por posibles perdidas de datos.

### 2. Columnas y significado:

* mfamily: indica la familia de operaciones a la que pertenece.
* mgroup: indica el grupo de operaciones dentro de la familia.
* mtype: indica el tipo de operación dentro del grupo.
* origin_trade_number: indica el número de la operación de trading (la misma operación puede tener varios números de trading).
* origin_contract_number: indica el número de contrato de la operación (igual para todas las operaciones que pertenecen al mismo contrato).
* maturity: fecha de finalización del contrato de cada operación.

### 3. Descripción del problema:

En estos datasets se encuentran varias operaciones financieras de distinto tipo, que diferenciaremos mediante los distintos valores de las columnas mfamily, mgroup y mtype.

Existe un cierto tipo de operaciones especiales, llamadas FXSwaps. Estas pueden ser diferenciadas por medio de los siguientes valores:

**mfamily = CURR** \
**mgroup = FXD** \
**mtype = SWLEG**

Podemos ver en nuestro dataset que estas operaciones aparecen duplicadas, es decir, con el mismo **origin_contract_number** aunque distinto **origin_trade_number**. De estas operaciones duplicadas en origen, queremos obtener solo una de ellas.

La forma para decidir cuál de las operaciones nos interesa obtener es mediante la columna *maturity*. De ambas operaciones de trading (distinto origin_trade_number) para un mismo contrato (origin_contract_number), queremos obtener solo la *long leg*, es decir, la que tiene una mayor fecha de vencimiento (fecha más actual de la columna maturity).

Existe un cierto problema en nuestro dataset trade_details que tendremos que solucionar. Podemos ver que para algunas operaciones el campo maturity vendrá como *null*, es decir, sin informar. En estos casos, deberemos buscar esa operacion en el dataset trade_details_snapshot y el respectivo campo maturity para poder saber cuál de las dos operaciones es la *long leg* y filtrar la *short leg* 

**NOTA: Si se quiere conocer más el significado de estas operaciones financieras: https://es.wikipedia.org/wiki/Swap_(finanzas)**

### 4. Reto:

* Obtener un dataframe final donde tengamos todas las operaciones originales excepto los short leg de los contratos tipo FXSwap.
* Aunque usemos el valor de la columna maturity del dataset trade_details_snapshot en los casos que venga en la trade_details a *null*, en el dataframe final deberá venir con el valor original de trade_details.
* Hacerlo de la manera más eficiente posible a nivel computacional.