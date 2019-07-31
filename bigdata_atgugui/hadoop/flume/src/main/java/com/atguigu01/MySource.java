package com.atguigu01;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;
import java.util.Map;

/**
 * Create by chenqinping on 2019/3/23 11 27
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {

    //���������ļ�����Ҫ��ȡ���ֶ�
    private Long delay;
    private String field;


    @Override
    public Status process() throws EventDeliveryException {
        try {
            //�����¼�ͷ��Ϣ

            Map<String, String> headreMap = new HashMap<>();
            //�����¼�
            SimpleEvent event = new SimpleEvent();

            for (int i = 0; i < 5; i++) {

                //ѭ����װ�¼�
                //ͷ��Ϣ
                event.setHeaders(headreMap);
                //��������
                event.setBody((field+i).getBytes());

                //���¼�д��channel
                getChannelProcessor().processEvent(event);
                Thread.sleep(delay);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }

        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        delay = context.getLong("delay");
        field = context.getString("field", "Hello!");
    }


}
