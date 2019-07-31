package com.youfan.gmall1018.dw.pulisher.service;

import java.util.Map;

/**
 * Create by chenqinping on 2019/5/4 11:45
 */
public interface PublisherService {
    public int getDauTotal(String date);

    public Map getDauHourCount(String date);

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);

    public Map getSaleDetail(String date, String keyword, int startPage, int size, String aggFieldName, int aggsSize);


}
