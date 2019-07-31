package com.youfan.flume.interceptor.copy;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Create by chenqinping on 2019/4/19 10:48
 */
public class LogETLInterceptor implements Interceptor {


    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        byte[] body = event.getBody();

        String log = new String(body, Charset.forName("UTF-8"));

        if (log.contains("start")) {
            if (LogUtilss.validateStart(log)) {
                return event;
            }
        } else {
            if (LogUtilss.validateEvent(log)) {
                return event;
            }
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        return null;
    }

    @Override
    public void close() {

    }
}
