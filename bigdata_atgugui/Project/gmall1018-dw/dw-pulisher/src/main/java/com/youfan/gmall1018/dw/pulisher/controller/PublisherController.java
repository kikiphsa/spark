package com.youfan.gmall1018.dw.pulisher.controller;

import com.alibaba.fastjson.JSON;
import com.youfan.gmall1018.dw.pulisher.bean.Option;
import com.youfan.gmall1018.dw.pulisher.bean.Stat;
import com.youfan.gmall1018.dw.pulisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Create by chenqinping on 2019/5/4 11:46
 */
@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    @RequestMapping(value = "realtime-total", method = RequestMethod.GET)
    public String getTotal(@RequestParam("date") String date) {
        List totalList = new ArrayList();
        int dauTotal = publisherService.getDauTotal(date);
        Map dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        totalList.add(dauMap);

        Map newMidMap = new HashMap();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增用户");
        newMidMap.put("value", 2000);
        totalList.add(newMidMap);

        Double orderAmount = publisherService.getOrderAmount(date);
        Map orderAmountMap = new HashMap();
        orderAmountMap.put("id", "order_amount");
        orderAmountMap.put("name", "订单金额");
        orderAmountMap.put("value", orderAmount);

        totalList.add(orderAmountMap);

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {

        if ("dau".equals(id)) {
            //求今天的数据
            Map dauHourCountTDMap = publisherService.getDauHourCount(date);
            //昨天
            String yesterday = getYesterday(date);
            Map dauHourCountYDMap = publisherService.getDauHourCount(yesterday);

            //合并
            Map dauMap = new HashMap<>();
            dauMap.put("today", dauHourCountTDMap);
            dauMap.put("yesterday", dauHourCountYDMap);

            return JSON.toJSONString(dauMap);
        } else if ("order_amount".equals(id)) {
            //求今天的数据
            Map orderAmountHourTDMap = publisherService.getOrderAmountHour(date);
            //昨天
            String yesterday = getYesterday(date);
            Map orderAmountHourYDMap = publisherService.getOrderAmountHour(yesterday);

            //合并
            Map hashMap = new HashMap();
            hashMap.put("today", orderAmountHourTDMap);
            hashMap.put("yesterday", orderAmountHourYDMap);

            return JSON.toJSONString(hashMap);
        }

        return null;
    }

    public String getYesterday(String date) {
        Date today = null;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        try {
            today = simpleDateFormat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Date yesterday = DateUtils.addDays(today, -1);
        String format = simpleDateFormat.format(yesterday);

        return format;

    }

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date, @RequestParam("startpage") int startpage, @RequestParam("size") int size, @RequestParam("keyword") String keyword) {

        Map genderMap = publisherService.getSaleDetail(date, keyword, startpage, size, "user_gender", 2);

        int total = (int) genderMap.get("total");

        Map genderAggsMap = (Map) genderMap.get("aggs");
        //求性别占比
        Long maleCount = (Long) genderAggsMap.get("M");
        Long womanCount = (Long) genderAggsMap.get("F");

        double maleRate = Math.round(maleCount * 1000D / total) / 10D;
        double womanRate = Math.round(womanCount * 1000D / total) / 10D;

        List<Option> genderOption = new ArrayList<>();
        genderOption.add(new Option("男", maleRate));
        genderOption.add(new Option("女", womanRate));
        Stat genderStat = new Stat();
        genderStat.setOptions(genderOption);
        genderStat.setTitle("用户性别占比");

        Map ageMap = publisherService.getSaleDetail(date, keyword, startpage, size, "user_age", 100);

        Map ageAggMap = (Map) ageMap.get("aggs");

        int age20Count = 0;
        int age230Count = 0;
        int age30Count = 0;
        for (Object obj : ageAggMap.entrySet()) {

            Map.Entry entry = (Map.Entry) obj;
            String key = (String) entry.getKey();
            int age = Integer.parseInt(key);
            Long count = (Long) entry.getValue();

            if (age < 20) {
                age20Count += count;
            } else if (age >= 20 && age <= 30) {
                age230Count += count;
            } else {
                age30Count += count;
            }
        }

        //每个年龄段占比
        double age20Rate = Math.round(age20Count * 1000D / total) / 10D;
        double age23Rate = Math.round(age230Count * 1000D / total) / 10D;
        double age30Rate = Math.round(age30Count * 1000D / total) / 10D;

        List<Option> options = new ArrayList<>();
        options.add(new Option("20岁以下的", age20Rate));
        options.add(new Option("20到30岁的", age23Rate));
        options.add(new Option("30岁以上的", age30Rate));

        Stat ageStat = new Stat();
        ageStat.setOptions(options);
        ageStat.setTitle("用户年龄占比");


        List<Stat> stats = new ArrayList<>();
        stats.add(genderStat);
        stats.add(ageStat);

        Map saleMap = new HashMap<>();
        saleMap.put("total", total);
        saleMap.put("stat", stats);
        saleMap.put("detail", genderMap.get("detail"));


        return JSON.toJSONString(saleMap);


    }


}
