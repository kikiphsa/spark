package com.atguigu01;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * Create by chenqinping on 2019/3/23 14 26
 */
public class SQLSource extends AbstractSource implements Configurable, PollableSource {

    //��ӡ��־
    private static final Logger LOG = LoggerFactory.getLogger(SQLSource.class);
    //����sqlHelper
    private SQLSourceHelper sqlSourceHelper;


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
        try {
            //��ʼ��
            sqlSourceHelper = new SQLSourceHelper(context);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        try {
            //��ѯ���ݱ�
            List<List<Object>> result = sqlSourceHelper.executeQuery();
            //���event�ļ���
            List<Event> events = new ArrayList<>();
            //���eventͷ����
            HashMap<String, String> header = new HashMap<>();
            //����з������ݣ������ݷ�װΪevent
            if (!result.isEmpty()) {
                List<String> allRows = sqlSourceHelper.getAllRows(result);
                Event event = null;
                for (String row : allRows) {
                    event = new SimpleEvent();
                    event.setBody(row.getBytes());
                    event.setHeaders(header);
                    events.add(event);
                }
                //��eventд��channel
                this.getChannelProcessor().processEventBatch(events);
                //�������ݱ��е�offset��Ϣ
                sqlSourceHelper.updateOffset2DB(result.size());
            }
            //�ȴ�ʱ��
            Thread.sleep(sqlSourceHelper.getRunQueryDelay());
            return Status.READY;
        } catch (InterruptedException e) {
            LOG.error("Error procesing row", e);
            return Status.BACKOFF;
        }
    }

    @Override
    public synchronized void stop() {
        LOG.info("Stopping sql source {} ...", getName());
        try {
            //�ر���Դ
            sqlSourceHelper.close();
        } finally {
            super.stop();
        }
    }
}

