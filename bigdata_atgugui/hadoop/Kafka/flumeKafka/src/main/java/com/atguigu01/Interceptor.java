package com.atguigu01;

import org.apache.flume.Context;
import org.apache.flume.Event;

import java.util.List;

/**
 * Create by chenqinping on 2019/3/27 14 50
 */
public class Interceptor implements org.apache.flume.interceptor.Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();

        if ('1' <= event.getBody()[0] && event.getBody()[0] > '9') {
            event.getHeaders().put("topic", "number");
        } else if ('a' <= event.getBody()[0] && event.getBody()[0] <= 'z') {
            event.getHeaders().put("topic", "letter");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        return null;
    }

    @Override
    public void close() {

    }

    public static class CustomBuilder implements org.apache.flume.interceptor.Interceptor.Builder {

        @Override
        public org.apache.flume.interceptor.Interceptor build() {
            return new Interceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
