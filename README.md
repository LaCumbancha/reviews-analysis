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

Para correr el sistema deberá ejecutarse el comando:

```bash
make docker-compose-up
```

Así mismo, para poder tener un seguimiento del mismo a través de los logs, se deberá utilizar:

```bash
make docker-compose-logs
```

Dentro del directorio `scripts` scripts se podrá encontrar el archivo de configuración `system-config.yaml` con el que podrá jugar con las distintas partes del sistema. Por un lado, dentro de las secciones de cada flujo podrá editarse la cantidad de nodos de cada uno de los componentes. Por otro lado, en las 2 primeras secciones se podrán editar configuraciones básicas del sistema:

* `testing_mode`: Define si el sistema correrá con el set de datos productivo o con uno de testing.
* `test_file_size`: En caso de que se corra en modo test, se podrá especificar el tamaño del set de datos que se generará con el script `test-builder`.
* `reviews_pool_size`: Especifíca el tamaño del pool de gorutinas utilizadas por el ReviewsScatter para enviar mensajes al Rabbit.
* `business_pool_size`: Especifíca el tamaño del pool de gorutinas utilizadas por el BusinessesScatter para enviar mensajes al Rabbit.
* `users_min_reviews`: Especifíca el mínimo de reviews requeridas para filtrar usuarios, permitiéndo ver usuarios con +50 mensajes u otro valor.
* `bots_min_reviews`: Especifíca el mínimo de reviews requeridas para filtrar bots, permitiéndo ver usuarios con +5 mensajes con el mismo texto u otro valor.
