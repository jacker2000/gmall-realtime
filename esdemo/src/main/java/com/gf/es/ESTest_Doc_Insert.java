package com.gf.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class ESTest_Doc_Insert {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient esClient = new RestHighLevelClient(RestClient.builder(
                new HttpHost("hadoop102", 9200, "http")));

        //插入数据
        IndexRequest request = new IndexRequest();
        request.index("user").id("1001");

        User user = new User();
        user.setName("gf");
        user.setAge(30);
        user.setSex("男");

        //向es插入数据，必须将数据转换为JSON格式
        ObjectMapper mapper = new ObjectMapper();
        String userJson = mapper.writeValueAsString(user);
        request.source(userJson, XContentType.JSON);
        IndexResponse response =
                esClient.index(request, RequestOptions.DEFAULT);

        System.out.println(response.getResult());

        esClient.close();
    }
}
