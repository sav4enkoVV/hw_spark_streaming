# hw_spark_streaming
C помощью KafkaProducerApp генерируется показатели температуры двигателя машины в виде структуры json, затем данные передаются в SparkApp, который в свою очередь рассчитывает среднюю температуру в интервале 10 секунд.
