package com.keli.platform.compact.mongo;

import com.mongodb.*;
import com.typesafe.config.Config;
import org.bson.codecs.BigDecimalCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MongoDBClientUtils {
    // 记录日志
    private static final Logger log = LoggerFactory.getLogger(MongoDBClientUtils.class);

    public static MongoClient client(Config conf){
        MongoClient mongoDb = null;
        // 数据库连接实例
        try {
            // 连接到MongoDB服务 如果是远程连接可以替换 localhost 为服务器所在IP地址
            // ServerAddress()两个参数分别为服务器地址 和 端口
            ServerAddress serverAddress = new ServerAddress(
                    conf.getString("mongo.ip"),conf.getInt("mongo.port"));
            List<ServerAddress> addrs = new ArrayList<ServerAddress>();
            addrs.add(serverAddress);

            // MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
            MongoCredential credential = MongoCredential.createScramSha1Credential(
                    conf.getString("mongo.username"),
                    conf.getString("mongo.admin.name"),
                    conf.getString("mongo.password").toCharArray());
            List<MongoCredential> credentials = new ArrayList<MongoCredential>();
            credentials.add(credential);

            // 通过连接认证获取MongoDB连接
            mongoDb = new MongoClient(addrs,credentials);

        } catch (MongoException e) {
            log.error("failed to connect mongodb", e);
        }

        return mongoDb;
    }
}
