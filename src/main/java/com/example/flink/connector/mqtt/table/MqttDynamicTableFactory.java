package com.example.flink.connector.mqtt.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.*;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

public class MqttDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    /**
     * TODO　１、创建动态表source
     * DynamicTableFactory需要具备以下功能：
     *      -定义与校验建表时传入的各项参数；
     *      -获取表的元数据；
     *      -定义读写数据时的编码/解码格式（非必需）；
     *      -创建可用的DynamicTable[Source/Sink]实例。
     */
    public DynamicTableSource createDynamicTableSource(Context context) {
        //内置工具类校验传入参数
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        // 获取有效参数
        final ReadableConfig options = helper.getOptions();

        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);


        // 获取元数据信息
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        // 创建并且返回一个动态表源
        return new MqttDynamicTableSource(options, decodingFormat, resolvedSchema);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        //内置工具类校验传入参数
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        // 获取有效参数
        final ReadableConfig options = helper.getOptions();

        final EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
                SerializationFormatFactory.class,
                FactoryUtil.FORMAT);


        // 获取元数据信息
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        // 创建并且返回一个动态表源
        return new MqttDynamicTableSink(options, encodingFormat, resolvedSchema);
    }

    @Override
    //TODO　２、指定工厂类的标识符，该标识符就是建表时必须填写的connector参数的值
    public String factoryIdentifier() {
        return "mqtt";
    }

    @Override
    //TODO 3、with里面必须要填写的属性配置
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTURL);
        options.add(TOPIC);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(FactoryUtil.FORMAT); // use pre-defined option for format
        return options;
    }

    @Override
    //TODO　４、with里面非必须填写属性配置
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CLIENTID);
        return options;
    }

    //TODO 5、定义MQTT Connector需要的各项参数
    public static final ConfigOption<String> HOSTURL =
            ConfigOptions.key("hosturl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mqtt's connect hosturl.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mqtt's connect username.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mqtt's connect password.");

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the mqtt's connect topic.");

    public static final ConfigOption<String> CLIENTID =
            ConfigOptions.key("clientid")
                    .stringType()
                    .defaultValue(String.valueOf(UUID.randomUUID()))
                    .withDescription("the mqtt's connect clientId.");


}
