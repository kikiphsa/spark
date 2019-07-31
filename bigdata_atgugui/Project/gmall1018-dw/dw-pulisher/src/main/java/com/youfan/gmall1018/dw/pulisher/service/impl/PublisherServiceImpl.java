package com.youfan.gmall1018.dw.pulisher.service.impl;

import com.youfan.gmall1018.dw.common.constant.GmallConstant;
import com.youfan.gmall1018.dw.pulisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Create by chenqinping on 2019/5/4 11:46
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    JestClient jestClient;

    @Override
    public int getDauTotal(String date) {

        int total = 0;
        //利用构造工具组合dsl
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermsQueryBuilder("logDate", date));
        searchSourceBuilder.query(boolQueryBuilder);
        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU)
                .addType(GmallConstant.ES_DEFAULT_TYPE).build();

        try {
            SearchResult execute = jestClient.execute(search);
            total = execute.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return total;
    }

    @Override
    public Map getDauHourCount(String date) {
        Map dauHourMap = new HashMap();


        //利用构造工具组合dsl
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤部分
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate", date));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合部分
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        searchSourceBuilder.aggregation(termsBuilder);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_DEFAULT_TYPE).build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            //遍历得到每个小时的累计值
            for (TermsAggregation.Entry bucket : buckets) {
                dauHourMap.put(bucket.getKey(), bucket.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dauHourMap;
    }

    /**
     * 求订单总金额方法
     *
     * @param date
     * @return
     */
    @Override
    public Double getOrderAmount(String date) {
        Double orderAmount=0D;
        // 查询 1 过滤  2 聚合
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalamount").field("totalAmount");
        searchSourceBuilder.aggregation(sumBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType(GmallConstant.ES_DEFAULT_TYPE).build();

        try {
            SearchResult searchResult = jestClient.execute(search);
            orderAmount = searchResult.getAggregations().getSumAggregation("sum_totalamount").getSum();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return orderAmount;
    }

    /**
     * 求订单真实金额
     *
     * @param date
     * @return
     */
    @Override
    public Map getOrderAmountHour(String date) {
        Map orderAmountMap = new HashMap<>();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermsQueryBuilder("createDate", date));
        searchSourceBuilder.query(boolQueryBuilder);

        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_createHour").field("createHour").size(24);

        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalamount").field("totalAmount");
        termsBuilder.subAggregation(sumBuilder);

        searchSourceBuilder.aggregation(termsBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType(GmallConstant.ES_DEFAULT_TYPE).build();

        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_createHour").getBuckets();

            for (TermsAggregation.Entry bucket : buckets) {
                String key = bucket.getKey();
                Double totalamount = bucket.getSumAggregation("sum_totalamount").getSum();
                orderAmountMap.put(key, totalamount);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        return orderAmountMap;
    }

    @Override
    public Map getSaleDetail(String date, String keyword, int startPage, int pageSize, String aggFieldName, int aggsSize) {

        Map saleDetailMap = new HashMap();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermsQueryBuilder("dt",date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));

        searchSourceBuilder.query(boolQueryBuilder);

        //聚合操作
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_" + aggFieldName).field(aggFieldName).size(aggsSize);
        searchSourceBuilder.aggregation(termsBuilder);

        //分页
        searchSourceBuilder.from((startPage-1)* pageSize);
        searchSourceBuilder.size(pageSize);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE).addType(GmallConstant.ES_DEFAULT_TYPE).build();

        try {
            SearchResult searchResult = jestClient.execute(search);
            //取出明细数据
            List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
           List<HashMap> detailMap = new ArrayList<>();
            for (SearchResult.Hit<HashMap, Void> hit : hits) {
                HashMap sourceMap = hit.source;
                detailMap.add(sourceMap);
            }
            saleDetailMap.put("detail",detailMap);

            //取出聚合结果
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_" + aggFieldName).getBuckets();

            HashMap aggsMap = new HashMap();
            for (TermsAggregation.Entry bucket : buckets) {
                aggsMap.put(bucket.getKey(),bucket.getCount());
            }
            saleDetailMap.put("aggs",aggsMap);

            //获取总数
            saleDetailMap.put("total",searchResult.getTotal());
        } catch (IOException e) {
            e.printStackTrace();
        }


        return saleDetailMap;
    }

}
