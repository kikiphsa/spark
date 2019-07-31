package persto;

import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoResultSet;
import com.facebook.presto.jdbc.PrestoStatement;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Properties;

/**
 * @Auther: Shuny
 * @Date: 2019/7/25 18:10
 * @Description:
 */
public class loadData {
    static String className = loadData.class.getName();
    static Logger log = Logger.getLogger(className);

    public static void main(String[] args) throws Exception {
        PropUtil.initial("datajdbc.properties");
        Properties props = PropUtil.getProp();
        log.error("程序参数:");
        for (String key : props.stringPropertyNames()) {
            log.error(key + "=" + props.getProperty(key));
        }


        startLoadDataStreaming(props);
    }

    private static void startLoadDataStreaming(Properties props) {
        PrestoConnection con = null;
        PreparedStatement preparedStatement = null;
        PrestoResultSet resultSet = null;
        Statement sta = null;
        PrestoStatement psta = null;


        try {
            Class.forName(props.getProperty("driver"));
            con = (PrestoConnection) DriverManager.getConnection(props.getProperty("databaseurl"), props.getProperty("username"),
                    props.getProperty("password"));
//            con = DriverManager.getConnection("jdbc:presto://10.246.184.88:8082/rec/yjjy?SSL=true&user=bjsy&password" +
//                    "=bjsy");
            System.out.println(con);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String sql = "select memcardnum,ystotal ,  createtime  from oc_userorderinfo";
        try {
            psta = (PrestoStatement) con.createStatement();

//            resultSet = sta.executeQuery("select memcardnum,ystotal ,  createtime  from oc_userorderinfo");
            resultSet = (PrestoResultSet) psta.executeQuery("select * from oc_userorderinfo");
            System.out.println(resultSet);
            while (resultSet.next()) {
                String memcardnum = resultSet.getString("memcardnum");
                int ystotal = resultSet.getInt("ystotal");
                Timestamp createtime = resultSet.getTimestamp("createtime");

                System.out.println(memcardnum + "   " + ystotal + "   " + createtime);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (sta != null) {
                try {
                    sta.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }
    }
}
