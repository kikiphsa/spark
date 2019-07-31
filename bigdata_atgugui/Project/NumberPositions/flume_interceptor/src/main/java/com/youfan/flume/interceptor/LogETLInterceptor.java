package com.youfan.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Create by chenqinping on 2019/4/19 10:29
 */
public class LogETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //etl
        byte[] body = event.getBody();


        String log = new String(body, Charset.forName("UTF-8"));

        //校验
        if (log.contains("start")){
            if (LogUtils.validateStart(log)){
                return event;
            }
        }else{
            if (LogUtils.validateEvent(log)){
                return event;
            }
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
       List<Event> intercepts = new ArrayList<>();
        for (Event event : events) {
            Event intercept = intercept(event);
            if (intercept != null){
                intercepts.add(intercept);
            }
        }
        return intercepts;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
