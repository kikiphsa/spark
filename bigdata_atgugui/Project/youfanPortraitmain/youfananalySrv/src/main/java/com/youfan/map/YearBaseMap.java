package com.youfan.map;

import com.youfan.entity.YearBase;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Create by chenqinping on 2019/5/13 23:25
 */
public class YearBaseMap implements MapFunction<String, YearBase> {
    @Override
    public YearBase map(String s) throws Exception {

        if (StringUtils.isBlank(s)) {
            return null;
        }
        String[] userInfo = s.split(",");
        String userid = userInfo[0];


        return null;
    }
}
