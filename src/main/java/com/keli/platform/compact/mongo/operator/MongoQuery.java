package com.keli.platform.compact.mongo.operator;

import com.keli.platform.compact.bean.SensorAlign;
import com.keli.platform.compact.bean.SensorThreshold;
import com.keli.platform.compact.mongo.MongoDBClientUtils;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Objects;

public class MongoQuery {
    private static final Logger log = LoggerFactory.getLogger(MongoQuery.class);

    private MongoClient client;
    private final Config conf;

    /**
     * 构造函数
     * @param config:默认配置参数
     */
    public MongoQuery(Config config){
        this.conf = config;
    }

    /**
     * 关闭数据库
     */
    private void close(){
        this.client.close();
    }


    /**
     * 查询校准表中的数据。返回所有设备最近的标定信息
     * 标定信息包括：时间戳、同轴信息、标定阈值；
     * 其中标定阈值包括均值、上偏差、下偏差
     * @return sensorAlignMap:map对象。key表示dev_id,value表示sensorAlign标定信息
     */
    public SensorAlign queryAlign(String dev_id) {
        // 1、获取数据库连接实例
        client = MongoDBClientUtils.client(this.conf);
        // 2、获取操作数据库
        MongoDatabase mgdb = client.getDatabase(this.conf.getString("mongo.database"));
        // 3、获取指定集合
        MongoCollection<Document> collection = mgdb.getCollection(this.conf.getString("mongo.query.align.collection"));

        SensorAlign sensorAlign = null;
        try{
            // 4、查找给定dev_id的数据
            Document query = new Document("dev_id", dev_id.trim());
            // 按timestamp降序排列
            FindIterable<Document> documents = collection.find(query).sort(Sorts.descending("timestamp"));
            //System.out.println(documents.first().toJson());
            sensorAlign = parseDocument(Objects.requireNonNull(documents.first()));
        } catch (NullPointerException e){
            log.error("dev_id=" + dev_id + "数据为空");
        } finally {
            // 5、关闭数据库
            this.close();
        }

        return sensorAlign;
    }

    /**
     * 解析从mongo查询的数据(dev_id最近的一条标定信息)并包装成sensorAlign标定信息对象。
     * @param document:从mongo查询的数据
     * @return sensorAlign:标定信息对象
     */
    private SensorAlign parseDocument(Document document){
        // 获取dev_id
        String dev_id = document.getString("dev_id");
        // 获取时间戳
        String timestamp = document.getString("timestamp");
        // 获取同轴信息
        String axis = document.getString("axis");

        // 获取标定阈值信息
        ArrayList<SensorThreshold> alignData = new ArrayList<SensorThreshold>();
        ArrayList<Document> data = (ArrayList<Document>) document.get("data");
        for (Document d : data){
            alignData.add(new SensorThreshold(
                    d.getString("sensor_id"),
                    d.getDouble("mean"),
                    d.getDouble("upper_thre"),
                    d.getDouble("lower_thre")
            ));
        }
        return new SensorAlign(dev_id,timestamp,axis,alignData);
    }

    public static void main(String[] args) {
        Config conf = ConfigFactory.load("default.properties");
        String dev_id = "862952024249400";
        MongoQuery query = new MongoQuery(conf);
        SensorAlign sensorAlign = query.queryAlign(dev_id);

        System.out.println(sensorAlign);
    }
}
