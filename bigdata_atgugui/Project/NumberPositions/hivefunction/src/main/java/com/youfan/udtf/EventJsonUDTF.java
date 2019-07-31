package com.youfan.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

/**
 * Create by chenqinping on 2019/4/20 16:15
 */
public class EventJsonUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {


        List<String> findNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        findNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        findNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(findNames, fieldOIs);

    }

    @Override
    public void process(Object[] objects) throws HiveException {

        String input = objects[0].toString();

        if (StringUtils.isBlank(input)) {
            return;
        } else {

            try {
                JSONArray ja = new JSONArray(input);
                if (ja == null) {
                    return;
                }

                for (int i = 0; i < ja.length(); i++) {

                    String[] result = new String[2];

                    try {
                        result[0] = ja.getJSONObject(i).getString("en");
                        result[1] = ja.getString(i);
                    } catch (JSONException e) {
                        continue;
                    }

                    forward(result);
                }

            } catch (JSONException e) {
                e.printStackTrace();
            }

        }


    }


    @Override
    public void close() throws HiveException {

    }
}
