package com.example.flink.connector.mqtt.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;

import static com.example.flink.connector.mqtt.table.MqttOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

public class MqttDynamicTableSink implements DynamicTableSink {
    private ReadableConfig options;
    private ResolvedSchema schema;
    private EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    public MqttDynamicTableSink(ReadableConfig options, EncodingFormat<SerializationSchema<RowData>> encodingFormat, ResolvedSchema schema) {
        this.options = options;
        this.encodingFormat = encodingFormat;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> serializer = encodingFormat.createRuntimeEncoder(
                context,
                schema.toPhysicalRowDataType());
        String hostUrl = this.options.get(HOST_URL);
        String username = this.options.get(USERNAME);
        String password = this.options.get(PASSWORD);
        String topics = this.options.get(SINK_TOPICS);
        Integer qos = this.options.get(QOS);
        String clientIdPrefix = this.options.get(CLIENT_ID_PREFIX);
        Integer connectionTimeout = this.options.get(CONNECTION_TIMEOUT);
        Integer keepAliveInterval = this.options.get(KEEP_ALIVE_INTERVAL);
        boolean automaticReconnect = this.options.get(AUTOMATIC_RECONNECT);
        final SinkFunction<RowData> sinkFunction = new MqttSinkFunction<>(hostUrl, username, password, topics, qos, clientIdPrefix, connectionTimeout, keepAliveInterval, automaticReconnect, serializer);

        Integer sinkParallelism = this.options.get(SINK_PARALLELISM);
        return SinkFunctionProvider.of(sinkFunction, sinkParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new MqttDynamicTableSink(options, encodingFormat, schema);
    }

    @Override
    public String asSummaryString() {
        return "Mqtt Table Sink";
    }
}
