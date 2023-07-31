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
        final SinkFunction<RowData> sinkFunction = new MqttSinkFunction<>(options, serializer);
        return SinkFunctionProvider.of(sinkFunction);
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
