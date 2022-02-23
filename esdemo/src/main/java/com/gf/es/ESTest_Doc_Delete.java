package com.gf.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class ESTest_Doc_Delete {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient esClient = new RestHighLevelClient(RestClient.builder(
                new HttpHost("hadoop102", 9200, "http")));
        DeleteRequest request = new DeleteRequest();
        request.index("user").id("1001");
        DeleteResponse response =
                esClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
        esClient.close();
    }
}
