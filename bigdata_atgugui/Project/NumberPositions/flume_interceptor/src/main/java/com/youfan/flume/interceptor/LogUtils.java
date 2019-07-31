package com.youfan.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;

/**
 * Create by chenqinping on 2019/4/19 10:34
 */
public class LogUtils {

// 1555642009009|{"cm":{"ln":"-103.4","sv":"V2.1.3","os":"8.2.9","g":"J53B5II2@gmail.com","mid":"971","nw":"4G","l":"es","vc":"5","hw":"640*1136","ar":"MX","uid":"971","t":"1555623660921","la":"-30.8","md":"sumsung-12","vn":"1.2.8","ba":"Sumsung","sr":"F"},"ap":"app","et":[{"ett":"1555613411948","en":"newsdetail","kv":{"entry":"2","goodsid":"242","news_staytime":"6","loading_time":"5","action":"2","showtype":"1","category":"11","type1":""}},{"ett":"1555603971750","en":"loading","kv":{"extend2":"","loading_time":"6","action":"3","extend1":"","type":"3","type1":"","loading_way":"2"}},{"ett":"1555572803904","en":"ad","kv":{"entry":"2","show_style":"4","action":"2","detail":"","source":"2","behavior":"2","content":"1","newstype":"1"}},{"ett":"1555578154891","en":"notification","kv":{"ap_time":"1555610791258","action":"1","type":"4","content":""}},{"ett":"1555545427445","en":"active_foreground","kv":{"access":"1","push_id":"1"}},{"ett":"1555636733298","en":"error","kv":{"errorDetail":"at cn.lift.dfdfdf.control.CommandUtil.getInfo(CommandUtil.java:67)\\n at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\n at java.lang.reflect.Method.invoke(Method.java:606)\\n","errorBrief":"at cn.lift.dfdf.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)"}},{"ett":"1555563357752","en":"comment","kv":{"p_comment_id":1,"addtime":"1555618869692","praise_count":851,"other_id":4,"comment_id":1,"reply_count":108,"userid":8,"content":"钾泅阎羞"}},{"ett":"1555629142039","en":"favorites","kv":{"course_id":0,"id":0,"add_time":"1555562277654","userid":4}}]}

    public static boolean validateEvent(String log) {
        // 服务器时间 | json
        // 1549696569054 | {"cm":{"ln":"-89.2","sv":"V2.0.4","os":"8.2.0","g":"M67B4QYU@gmail.com","nw":"4G","l":"en","vc":"18","hw":"1080*1920","ar":"MX","uid":"u8678","t":"1549679122062","la":"-27.4","md":"sumsung-12","vn":"1.1.3","ba":"Sumsung","sr":"Y"},"ap":"weather","et":[]}

        // 1 切割
        String[] logContents = log.split("\\|");


        // 2 校验
        if (logContents.length != 2) {
            return false;
        }

        //3 校验服务器时间
        if (logContents[0].length() != 13 || !NumberUtils.isDigits(logContents[0])) {
            return false;
        }

        // 4 校验json
        if (!logContents[1].trim().startsWith("{") || !logContents[1].trim().endsWith("}")) {
            return false;
        }

        return true;
    }

    public static boolean validateStart(String log) {

        if (log == null) {
            return false;
        }

        // 校验json
        if (!log.trim().startsWith("{") || !log.trim().endsWith("}")) {
            return false;
        }

        return true;
    }

}
