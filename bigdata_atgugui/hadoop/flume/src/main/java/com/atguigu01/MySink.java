package com.atguigu01;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create by chenqinping on 2019/3/23 12 41
 */
public class MySink extends AbstractSink implements Configurable {


    //����Logger����
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);

    private String prefix;
    private String suffix;

    @Override
    public Status process() throws EventDeliveryException {
        Status status;
        //��ȡ��ǰSikn�󶨵�Channel
        Channel channel = getChannel();

        //��ȡ����
        Transaction transaction = channel.getTransaction();

        //�����¼�
        Event event;
        //��������
        transaction.begin();
        //��ȡChannel�е��¼���ֱ����ȡ���¼�����ѭ��
        while (true) {
            event = channel.take();
            if (event != null) {
                break;
            }
        }

        try {
            //�����¼�����ӡ��
            LOG.info(prefix + new String(event.getBody()) + suffix);
            //�����ύ
            transaction.commit();
            status = Status.READY;
        } catch (Exception e) {
            e.printStackTrace();
            //�����쳣������ع�
            transaction.rollback();
            status = Status.BACKOFF;
        } finally {

            //�ر�����
            transaction.close();
        }


        return status;
    }

    @Override
    public void configure(Context context) {

        //��ȡ�����ļ�����,��Ĭ��ֵ
        prefix = context.getString("prefix", "limerde");
        suffix = context.getString("suffix");
    }
}
