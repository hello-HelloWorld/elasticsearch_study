package com.study;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/3/23 14:27
 */

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;

import javax.sound.midi.Soundbank;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Test {

    //获取Transport Client
    //ElasticSearch服务默认端口9300。
    //Web管理平台端口9200。
    private TransportClient client;

    @SuppressWarnings("unchecked")
    @Before
    public void getClient() throws UnknownHostException {
        //设置连接的集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        //连接集群
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.11.89"), 9300));
        //打印集群名称
        System.out.println(client.toString());
    }

    //创建索引
    @org.junit.Test
    public void createIndex() {
        //创建索引
        client.admin().indices().prepareCreate("blob").get();
        //关闭连接
        client.close();
    }

    //删除索引
    @org.junit.Test
    public void deleteIndex() {
        client.admin().indices().prepareDelete("blob").get();
        client.close();
    }

    //新建文档（数据源为json串）
    @org.junit.Test
    public void createByJson() {
        //文档数据
        String json = "{" + "\"id\":\"1\"," + "\"title\":\"基于Lucene的搜索服务器\","
                + "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"" + "}";

        //创建文档
        // 当直接在ElasticSearch建立文档对象时，如果索引不存在的，默认会自动创建，映射采用默认方式。
        IndexResponse response = client.prepareIndex("blob2", "article", "1").setSource(json).execute().actionGet();

        //打印结果
        System.out.println("id:" + response.getId());
        System.out.println("version:" + response.getVersion());
        System.out.println("result:" + response.getResult());

        //关闭连接
        client.close();
    }

    // 新建文档（源数据map方式添加json）
    @org.junit.Test
    public void createIndexByMap() {
        // 1 文档数据准备
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("id", "2");
        json.put("title", "基于Lucene的搜索服务器");
        json.put("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");

        //2.创建文档
        IndexResponse response = client.prepareIndex("blob2", "article", "2").setSource(json).execute().actionGet();

        //3.打印结果
        System.out.println("id:" + response.getId());
        System.out.println("index:" + response.getIndex());
        System.out.println("type:" + response.getType());
        System.out.println("version:" + response.getVersion());
        System.out.println("result:" + response.getResult());

        //4.关闭资源
        client.close();
    }

    // 新建文档（源数据es构建器添加json）
    @org.junit.Test
    public void createIndexByes() throws IOException {
        // 1 通过es自带的帮助类，构建json数据
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("id", 3)
                .field("title", "基于Lucene的搜索服务器")
                .field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。")
                .endObject();

        //2.创建文档
        IndexResponse indexResponse = client.prepareIndex("blob2", "article", "3").setSource(builder).get();

        //3.打印返回结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        //4.关闭连接
        client.close();
    }

    // 搜索文档数据（单个索引）
    @org.junit.Test
    public void getData() {
        //1.查询文档
        GetResponse response = client.prepareGet("blob2", "article", "1").get();
        //2.打印结果
        System.out.println(response.getSourceAsString());
        //3.关闭连接
        client.close();
    }

    // 搜索文档数据（多个索引）
    @org.junit.Test
    public void getMultiData() {
        //1.查询多个文档
        MultiGetResponse response = client.prepareMultiGet().add("blob2", "article", "1")
                .add("blob2", "article", "1", "2")
                .add("blob2", "article", "3").get();
        //2.遍历返回的结果
        for (MultiGetItemResponse itemResponse : response) {
            GetResponse response1 = itemResponse.getResponse();
            //如果获取到结果
            if (response1.isExists()) {
                String sourceAsString = response1.getSourceAsString();
                System.out.println(sourceAsString);
            }
        }

        //3.关闭资源
        client.close();
    }

    // 更新文档数据（update）
    @org.junit.Test
    public void updateIndex() throws Exception {
        // 1 创建更新数据的请求对象
        UpdateRequest request = new UpdateRequest();
        request.index("blob2");
        request.type("article");
        request.id("2");

        request.doc(XContentFactory.jsonBuilder().startObject()
                // 对没有的字段添加, 对已有的字段替换
                .field("title", "基于Lucene的搜索服务器")
                .field("content",
                        "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。大数据前景无限")
                .field("createDate", "2017-8-22").endObject());

        //2.获更新后的值
        UpdateResponse indexResponse = client.update(request).get();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("create:" + indexResponse.getResult());

        //4.关闭资源
        client.close();
    }

    // 更新文档数据（upsert）
    // 设置查询条件, 查找不到则添加IndexRequest内容，查找到则按照UpdateRequest更新。
    @org.junit.Test
    public void upsertData() throws Exception {
        // 设置查询条件, 查找不到则添加
        IndexRequest indexRequest = new IndexRequest("blob2", "article", "5")
                .source(XContentFactory.jsonBuilder().startObject()
                        .field("title", "搜索服务器")
                        .field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。").endObject());

        // 设置更新, 查找到更新下面的设置
        UpdateRequest upsert = new UpdateRequest("blob2", "aricle", "5").doc(XContentFactory.jsonBuilder().startObject().field("user", "李四").endObject()).upsert(indexRequest);

        UpdateResponse updateResponse = client.update(upsert).get();
        client.close();
    }

    //删除文档数据
    @org.junit.Test
    public void deleteData() {
        //1.删除数据
        DeleteResponse indexResponse = client.prepareDelete("blob2", "article", "5").get();

        // 2 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("found:" + indexResponse.getResult());

        //3.关闭资源
        client.close();
    }

    // 查询所有（matchAllQuery）
    @org.junit.Test
    public void matchAllQuery() {
        //1.执行查询
        SearchResponse searchResponse = client.prepareSearch("blob2").setTypes("article").setQuery(QueryBuilders.matchAllQuery()).get();
        //2.查询结果有多少个对象
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            //每个查询对象
            SearchHit next = iterator.next();
            System.out.println(next.getSourceAsString());
        }

        //3.关闭连接
        client.close();
    }

    // 对所有字段分词查询（queryStringQuery）
    @org.junit.Test
    public void query() {
        //1.条件查询
        SearchResponse searchResponse = client.prepareSearch("blob2").setTypes("article").setQuery(QueryBuilders.queryStringQuery("全文")).get();
        //2.查询结果有多少对象
        SearchHits hits = searchResponse.getHits();
        System.out.println("有" + hits.getTotalHits() + "个对象");

        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit next = iterator.next();//每个查询对象
            System.out.println(next.getSourceAsString());
        }

        //3.关闭连接
        client.close();
    }

    // 通配符查询（wildcardQuery）
    //         *：表示多个字符（任意的字符）
    //         ？：表示单个字符
    @org.junit.Test
    public void getWildCardQuery() {
        //1.通配符查询
        SearchResponse searchResponse = client.prepareSearch("blob2").setTypes("article").setQuery(QueryBuilders.wildcardQuery("content", "*全*")).get();

        SearchHits hits = searchResponse.getHits();
        System.out.println(hits.getTotalHits());
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit next = iterator.next();
            System.out.println(next.getSourceAsString());
        }
        client.close();
    }

    //词条查询（TermQuery）
    @org.junit.Test
    public void termQuery() {
        SearchResponse searchResponse = client.prepareSearch("blob2").setTypes("article").setQuery(QueryBuilders.termQuery("content", "全")).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println(hits.getTotalHits());
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit next = iterator.next();
            System.out.println(next.getSourceAsString());
        }
        client.close();
    }

    // 模糊查询（fuzzy）
    @org.junit.Test
    public void fuzzy() {
        SearchResponse searchResponse = client.prepareSearch("blob2").setTypes("article").setQuery(QueryBuilders.fuzzyQuery("title", "lucence")).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println(hits.getTotalHits());
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit next = iterator.next();
            System.out.println(next.getSourceAsString());
        }
        client.close();
    }
}



