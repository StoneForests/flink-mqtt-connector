# flink-mqtt-connector
 使用flink1.14.3和paho mqtt客户端实现的自定义flink mqtt connector，分别使用table api和stream api进行了实现，可以从mqtt执行读取数据，写入数据。
 stream api的入口在MqttWordCount2MqttPaho.java，table api有两个入口，其中只读mqtt的入口是FlinkTableJustSource.java，又读又写mqtt的是FlinkTableSourceSink.java
