package com.jingxun;

import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * 存储id->词的数据
 */
public class SearchComputed6 {

    public static void main(String[] args) throws Exception {
        var instanceId = Integer.parseInt(args[0]);
        MongoClient[] clients = new MongoClient[3];
        MongoCollection<Document>[] collections = new MongoCollection[3];
        Arrays.fill(clients, null);
        Arrays.fill(collections, null);
        BiConsumer<String, Integer> fun = (url, index) -> {
            ConnectionString connectionString = new ConnectionString(url);
            MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();
            MongoClient mongoClient = MongoClients.create(settings);
            MongoDatabase database = mongoClient.getDatabase("test");
            clients[index] = mongoClient;
            collections[index] = database.getCollection("search_literature_words_map");
        };
        fun.accept("mongodb://192.168.98.101:27000", 0);
        fun.accept("mongodb://192.168.98.102:27000", 1);
        fun.accept("mongodb://192.168.98.103:27000", 2);
        SearchComputed.fileProcess(40, 400, (line, schema) -> {
            var orderId = (Number) SearchComputed.getParquetFieldValue(line, schema.getType("orderId"), schema.getFieldIndex("orderId"), 0);
            if ((orderId.intValue() & 1) != instanceId) return;
            var data = (List<String>) SearchComputed.getParquetFieldValue(line, schema.getType("data"), schema.getFieldIndex("data"), 0);
            var map = new HashMap<String,HashSet<String>>();
            for (var word: data) {
                var a = word.substring(0,1);
                var b = word.substring(1);
                var as = map.computeIfAbsent(a, v -> new HashSet<>());
                var bs = map.computeIfAbsent(b, v -> new HashSet<>());
                as.add(word);
                bs.add(word);
            }
            Document document = new Document("_id", orderId)
                    .append("id", orderId)
                    .append("data", map);
            collections[line.hashCode() % 3].insertOne(document);
        });
        for (var client : clients) client.close();
    }

}
