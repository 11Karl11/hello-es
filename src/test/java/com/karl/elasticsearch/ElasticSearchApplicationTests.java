package com.karl.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.karl.elasticsearch.pojo.User;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.omg.CORBA.ServerRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * es 7.6.X api测试
 */
@SpringBootTest
class ElasticSearchApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    //测试索引的创建
    @Test
    void contextLoads() throws IOException {
        //1.创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("karl_test");

        //2.客户端执行请求
        CreateIndexResponse createIndexResponse =
                client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);

    }

    //测试获取索引,判断其是否存在
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("karl_test");

        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }


    //测试删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("karl_test");

        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }


    //测试添加文档
    @Test
    void testAddDocument() throws IOException {
        //创建对象
        User user = User.builder().name("karl").age(29).build();

        //创建请求
        IndexRequest request = new IndexRequest("karl_index");

        //规则
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        //将我们的数据放入请求 json
        request.source(JSON.toJSONString(user), XContentType.JSON);

        //客户端发送请求
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println(indexResponse.toString());
        System.out.println(indexResponse.status());
    }

    //获取文档,判断是否存在 gew /index/doc/1
    @Test
    void testIsExists() throws IOException {

        GetRequest getRequest = new GetRequest("karl_index", "1");

        //不获取返回的_source的上下文了
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);


    }


    //获取文档信息
    @Test
    void testGetDocument() throws IOException {

        GetRequest getRequest = new GetRequest("karl_index", "1");

        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

        System.out.println(getResponse.getSourceAsString());


    }


    //更新文档信息
    @Test
    void testUpdateDocument() throws IOException {

        UpdateRequest updateRequest = new UpdateRequest("karl_index", "1");

        updateRequest.timeout("1s");

        User user = User.builder().name("updateName").age(3).build();

        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);

        System.out.println(updateResponse.status());


    }


    //删除文档信息
    @Test
    void testDeleteDocument() throws IOException {

        DeleteRequest deleteRequest = new DeleteRequest("karl_index", "1");

        deleteRequest.timeout("1s");

        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());


    }

    //特殊情况，批量插入数据

    @Test
    void testBulkRequest() throws Exception {

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<User> users = new ArrayList<>();
        users.add(User.builder().name("karl1").age(1).build());
        users.add(User.builder().name("karl2").age(2).build());
        users.add(User.builder().name("karl3").age(3).build());
        users.add(User.builder().name("karl4").age(4).build());
        users.add(User.builder().name("karl5").age(5).build());

        for (int i = 0; i < users.size(); i++) {
            //批量删除、批量更新修改这里
            bulkRequest.add(new IndexRequest("k_index")
                    .id("" + (i + 1))
                    .source(JSON.toJSONString(users.get(i)),XContentType.JSON));
        }
        BulkResponse responses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(responses.hasFailures());//是否失败，返回false代表成功
    }

    //查询
    //SearchRequest搜索请求    SearchSourceBuilder条件构造   HighlightBuilder构建高亮
    //TermQueryBuilder 构建精确查询
    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("k_index");

        //构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询条件我们可以用QueryBuilders工具来实现
        //精确查询
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "karl1");
        sourceBuilder.query(termQueryBuilder);

        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();

        System.out.println(JSON.toJSONString(hits));

        System.out.println("------------------------");

        for (SearchHit hit : hits.getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }


}
