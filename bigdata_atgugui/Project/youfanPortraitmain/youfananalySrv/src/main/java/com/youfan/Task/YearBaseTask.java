package com.youfan.Task;

import com.youfan.entity.YearBase;
import com.youfan.map.YearBaseMap;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Create by chenqinping on 2019/5/13 23:09
 */
public class YearBaseTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("input"));

        MapOperator<String, YearBase> map = text.map(new YearBaseMap());


    }
}
