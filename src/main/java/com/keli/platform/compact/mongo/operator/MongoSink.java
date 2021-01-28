package com.keli.platform.compact.mongo.operator;

import com.keli.platform.compact.SensorResult;
import com.keli.platform.compact.mongo.MongoDBClientUtils;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.typesafe.config.Config;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

public class MongoSink extends RichSinkFunction<SensorResult>{
    private MongoClient client;
    private MongoCollection<Document> collection;
    private Config conf = null;

    public MongoSink(Config config){
        this.conf = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 定义mongo连接
        client = MongoDBClientUtils.client(this.conf);
        // 定义操作数据库和操作集合
        collection = client.getDatabase(this.conf.getString("mongo.database"))
                .getCollection(this.conf.getString("mongo.sink.result.collection"));
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.client.close();
    }

    @Override
    public void invoke(SensorResult value, Context context) throws Exception {
        Document doc = new Document();
        // 设备id
        doc.append("dev_id",value.device_id());
        // 时间戳
        doc.append("time",value.time());
        // 同轴信息
        String coAxis = value.axis();
        // 同轴偏差
        String[] error = value.sensor_error();
        Document d = new Document();
        for ( String axis : coAxis.split(",") ){
            String id_1 = axis.split("-")[0];
            String id_2 = axis.split("-")[1];
            String value_1 = error[Integer.parseInt(id_1) - 1];
            String value_2 = error[Integer.parseInt(id_2) - 1];
            d.append(axis,new Document().append("error1",value_1).append("error2",value_2));
        }
        doc.append("data",d);

        collection.insertOne(doc);
    }
}
