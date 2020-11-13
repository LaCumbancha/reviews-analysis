# Review Analysis
TP2 | 75.74 - Sistemas Distribuidos I | 2C2020 | FIUBA

## Requerimientos 

### Funcionales

Se solicita un sistema distribuido que procese el detalle de críticas sobre el servicio de comercios. El sistema debe recibir la transmisión de los datos a ser procesados y retornar:
* Usuarios con 50 o más reviews.
* Usuarios con 50 o más reviews con comentarios 5 estrellas únicamente.
* Usuarios con 5 o más reviews que utilizan siempre el mismo texto.
A su vez, se desea obtener la siguiente información estadística:
* Histograma de cantidad de comentarios por día de la semana (Lu, Ma, ..., Do).
* Listado de las 10 ciudades más diverditas, es decir, con mayor cantidad de reviews "funny".
Como origen de datos se definen los archivos de ingreso registrados [en estos datasets](https://www.kaggle.com/pablodroca/yelp-review-analysis).

### No Funcionales

Además del correcto funcionamiento del sistema, deben tenerse en cuenta las siguientes consideraciones:

* El sistema debe estar optimizado para entornos multicomputadoras.
* EL sistema debe ser invocado desde un nodo que transmite los datos a ser procesados.
* Se debe soportar el escalamiento de los elementos de cómputo.
* De ser necesaria una comunicación basada en grupos, se requiere la definición de un middleware.
* El diseño debe permitir la adaptación para eventuales procesamientos en streaming, es decir, a medida que se reciben los nuevos comentarios.
* Debido a restricciones en el tiempo de implementación, se permite la construcción de un sistema acoplado al modelo de negocio. No es requerimiento la creación de una plataforma de procesamiento de datos.

## Desarrollo
