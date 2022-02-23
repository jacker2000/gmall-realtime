package com.gf.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.naming.directory.SearchResult;
import java.io.IOException;

public class ESTest_Doc_Query {

    RestHighLevelClient esClient =null;
    @Before
    public void init(){
         esClient = new RestHighLevelClient(RestClient.builder(
                new HttpHost("hadoop102", 9200, "http")));
    }

    /**
     *  1.查询索引中全部的数据
     */
    @Test
    public void getIndexAll() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     *  2.条件查询
     */
    @Test
    public void getCondition() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        request.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("age",30)));
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 3.分页查询
     */
    @Test
    public void getPageQuery() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder =
                new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        // (当前页码-1)*每页显示数据条数
        builder.from(2);
        builder.size(1);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 4.查询排序
     */
    @Test
    public void getSort() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder =
                new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        builder.sort("age", SortOrder.DESC);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println(response.getTook());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     *  5. 过滤字段
     */
    @Test
    public void getFilter() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        String[] excludes ={"age"};
        String [] includes ={};
        builder.fetchSource(includes,excludes);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     *  6. 组合查询
     */
    @Test
    public void getCombinationQuery() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("age",30));
        boolQueryBuilder.must(QueryBuilders.matchQuery("sex","男"));
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("sex","男"));
        boolQueryBuilder.should(QueryBuilders.matchQuery("age",30));
        boolQueryBuilder.should(QueryBuilders.matchQuery("age",40));

        builder.query(boolQueryBuilder);
        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     *  7. 范围查询
     */
    @Test
    public void getRangeQuery() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");
        rangeQuery.gte(40);
        rangeQuery.lte(50);
        builder.query(rangeQuery);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println(response.getTook());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 8. 模糊查询
     */
    @Test
    public void getFuzzyQuery() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.fuzzyQuery("name","wangwu").fuzziness(Fuzziness.TWO));
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     *  9. 高亮查询
     */
    @Test
    public void getHighlightQuery() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "wangwu");
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("name");
        builder.highlighter(highlightBuilder);
        builder.query(termQueryBuilder);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 10. 聚合查询
     *
     */
    @Test
    public void getAggregateQuery() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        MaxAggregationBuilder aggregationBuilder = AggregationBuilders.max("maxAge").field("age");
        builder.aggregation(aggregationBuilder);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * 11. 分组查询
     */
    @Test
    public void getGroupingQuery() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("user");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("ageGroup").field("age");
        builder.aggregation(aggregationBuilder);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println(hits.getTotalHits());
        System.out.println(response.getTook());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }
    @After
    public void close() throws IOException {
        esClient.close();
    }
}
