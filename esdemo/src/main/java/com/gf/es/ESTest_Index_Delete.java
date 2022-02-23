package com.gf.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class ESTest_Index_Delete {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient esClient = new RestHighLevelClient(RestClient.builder(
                new HttpHost("hadoop102", 9200, "http")));

        DeleteIndexRequest request = new DeleteIndexRequest("gf");
        AcknowledgedResponse response =
                esClient.indices().delete(request, RequestOptions.DEFAULT);

        //响应状态
        System.out.println(response.isAcknowledged());
        esClient.close();
    }
}
