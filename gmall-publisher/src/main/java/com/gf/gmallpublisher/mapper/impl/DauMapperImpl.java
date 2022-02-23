package com.gf.gmallpublisher.mapper.impl;

import com.gf.gmallpublisher.mapper.DauMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
@Slf4j
public class DauMapperImpl implements DauMapper {

    private String indexNamePrefix= "gmall_dau_info_";

    @Autowired
    RestHighLevelClient esClient;
    /**
     *  查总数
     * @param td
     * @return
     */
    private Long searchDauTotal(String td )   {
        String indexName=indexNamePrefix+td;

        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long dauTotal = response.getHits().getTotalHits().value;
            return dauTotal;
        } catch (IOException e) {
           throw new RuntimeException("查询日活总数失败");
        } catch (ElasticsearchException esException){
            if (esException.status()== RestStatus.NOT_FOUND) {
                log.error(indexName+"未建立");
            }
        }
        return 0L;
    }

    /**
     *  查询分时明细
     * @param td
     * @return
     */
    private Map<String,Long> searchDauHour(String td){
        String indexName=indexNamePrefix+td;

        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        sourceBuilder.size(0);
        TermsAggregationBuilder termsAggregationBuilder =
                AggregationBuilders.terms("groupByHr").field("hr").size(24);

        sourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = response.getAggregations();
            ParsedStringTerms groupByhr = aggregations.get("groupByHr");
            List<? extends Terms.Bucket> buckets = groupByhr.getBuckets();
             Map<String, Long> dauHour = new HashMap<>();
            for (Terms.Bucket bucket : buckets) {
                String hr = bucket.getKeyAsString();
                long docCount = bucket.getDocCount();
                dauHour.put(hr,docCount);
            }
         return dauHour;

        } catch (IOException e) {
            throw new RuntimeException("查询日活总数失败");
        } catch (ElasticsearchException esException){
            if (esException.status()== RestStatus.NOT_FOUND) {
                log.error(indexName+"未建立");
            }
            return  new HashMap<>();
        }
    }

    @Override
    public Map<String, Object> searchDauHourFromEs(String td) {
        Map<String, Object> results = new HashMap<>();
        //总数
        results.put("dauTotal",searchDauTotal(td));
        //今日分时明细
        results.put("dauTd",searchDauHour(td));
        //计算昨天日期
        LocalDate localDate = LocalDate.parse(td);
        LocalDate yd = localDate.minusDays(1);
        //昨日分时明细
        results.put("dauYd",searchDauHour(yd.toString()));
        return results;
//        Map<String, Long> dauYd = new HashMap<>();
//        dauYd.put("9",100L);
//        dauYd.put("10",34L);
//        dauYd.put("11",50L);
//        dauYd.put("12",200L);
//        dauYd.put("13",78L);
//        dauYd.put("14",100L);
//        dauYd.put("15",64L);
//        dauYd.put("16",120L);
//        dauYd.put("17",40L);
//        dauYd.put("18",80L);
//        Map<String, Integer> dauTd = new HashMap<>();
//        dauTd.put("8",100);
//        dauTd.put("11",34);
//        dauTd.put("13",68);
//        dauTd.put("15",254);
//        dauTd.put("17",40);
//
//        results.put("dauYd",dauYd);
//        results.put("dauTd",dauTd);

    }
}
