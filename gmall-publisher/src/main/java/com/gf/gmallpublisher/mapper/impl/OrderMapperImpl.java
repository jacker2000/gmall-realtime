package com.gf.gmallpublisher.mapper.impl;

import com.gf.gmallpublisher.bean.NameValue;
import com.gf.gmallpublisher.mapper.OrderMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
@Slf4j
public class OrderMapperImpl implements OrderMapper {
    @Autowired
    RestHighLevelClient esClient;
    private String indexNamePrefix = "gmall_order_wide_";

    @Override
    public Map<String, Object> searchDetailByItem(String itemName, String date, Integer pageNo, Integer pageSize) {
        //索引名
        String indexName = indexNamePrefix + date;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //query
        MatchQueryBuilder matchQuery =
                QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        sourceBuilder.query(matchQuery);

        //fetch
        sourceBuilder.fetchSource(new String[]{"sku_name","sku_num","province_name","order_price","total_amount","user_age","user_gender","create_time"},null);
        //分页
        int start= (pageNo-1)*pageSize;
        sourceBuilder.from(start);
        sourceBuilder.size(pageSize);

        //高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("sku_name");
        sourceBuilder.highlighter(highlightBuilder);

        searchRequest.source(sourceBuilder);
         Map<String, Object> resultMap = new HashMap<String, Object>();
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            //总条数
            long total = searchResponse.getHits().getTotalHits().value;
            //明细
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            List<Map<String,Object>> maps=new ArrayList<>();
            for (SearchHit searchHit : searchHits) {
                //明细
                Map<String, Object> source = searchHit.getSourceAsMap();
                //高亮
                Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
                HighlightField highlightField = highlightFields.get("sku_name");
                Text[] fragments = highlightField.getFragments();
                Text text = fragments[0];

                //替换
                source.put("sku_name",text.toString());
                maps.add(source);
            }
            //封装最终结果
            resultMap.put("total",total);
            resultMap.put("detail",maps);
            return resultMap;

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询es失败");
        } catch (ElasticsearchException esException) {
            if (esException.status() == RestStatus.NOT_FOUND) {
                log.error(indexName + "未建立");
            }
        }
        return null;
    }

    @Override
    public List<NameValue> searchStatsByItem(String itemName, String date, String field) {

        //索引名
        String indexName = indexNamePrefix + date;


        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //query
        MatchQueryBuilder matchQuery =
                QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        sourceBuilder.query(matchQuery);

        //分组
        // age     => user_age
        // gender  => user_gender
        TermsAggregationBuilder termsAggregationBuilder =
                AggregationBuilders.terms("groupby" + field).field(field).size(100);

        //聚合
        SumAggregationBuilder sumAggregationBuilder =
                AggregationBuilders.sum("sum_amount").field("split_total_amount");
        termsAggregationBuilder.subAggregation(sumAggregationBuilder);
        //分组
        sourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms aggregation = aggregations.get("groupby" + field);
            List<? extends Terms.Bucket> buckets = aggregation.getBuckets();
            ArrayList<NameValue> results = new ArrayList<>();
            for (Terms.Bucket bucket : buckets) {
                String name = bucket.getKeyAsString();
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedSum sum_amout = bucketAggregations.get("sum_amount");
                double value = sum_amout.getValue();
                NameValue nameValue = new NameValue(name, value);
                results.add(nameValue);
            }
            return results;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询es失败");
        } catch (ElasticsearchException esException) {
            if (esException.status() == RestStatus.NOT_FOUND) {
                log.error(indexName + "未建立");
            }
        }
        return new ArrayList<>();
    }


}
