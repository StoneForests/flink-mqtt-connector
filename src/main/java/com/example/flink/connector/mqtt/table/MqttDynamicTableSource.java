package com.example.flink.connector.mqtt.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import static com.example.flink.connector.mqtt.table.MqttOptions.*;

public class MqttDynamicTableSource implements ScanTableSource {
    private ReadableConfig options;
    private ResolvedSchema schema;
    private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    public MqttDynamicTableSource(ReadableConfig options, DecodingFormat<DeserializationSchema<RowData>> decodingFormat, ResolvedSchema schema) {
        this.options = options;
        this.decodingFormat = decodingFormat;
        this.schema = schema;
    }

    @Override
    //写入方式默认INSERT_ONLY,里面实现了一个static静态类初始化
    public ChangelogMode getChangelogMode() {
//        return decodingFormat.getChangelogMode();
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
//                .addContainedKind(RowKind.UPDATE_BEFORE)
//                .addContainedKind(RowKind.UPDATE_AFTER)
//                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    //获取运行时类
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext ctx) {

        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                ctx,
                schema.toPhysicalRowDataType());
        String broker = this.options.get(HOST_URL);
        String username = this.options.get(USERNAME);
        String password = this.options.get(PASSWORD);
        String topics = this.options.get(TOPICS);
        boolean cleanSession = this.options.get(CLEAN_SESSION);
        String clientIdPrefix = this.options.get(CLIENT_ID_PREFIX);
        Integer connectionTimeout = this.options.get(CONNECTION_TIMEOUT);
        Integer keepAliveInterval = this.options.get(KEEP_ALIVE_INTERVAL);
        boolean automaticReconnect = this.options.get(AUTOMATIC_RECONNECT);
        final SourceFunction<RowData> sourceFunction = new MqttSourceFunction<>(broker, username, password, topics, cleanSession, clientIdPrefix, automaticReconnect, connectionTimeout, keepAliveInterval, deserializer);
        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new MqttDynamicTableSource(options, decodingFormat, schema);
    }

    @Override
    public String asSummaryString() {
        return "Mqtt Table Source";
    }
}
