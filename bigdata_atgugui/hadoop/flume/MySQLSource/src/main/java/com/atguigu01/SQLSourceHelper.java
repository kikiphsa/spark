package com.atguigu01;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Create by chenqinping on 2019/3/23 14 24
 */
public class SQLSourceHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SQLSourceHelper.class);

    private int runQueryDelay, //���β�ѯ��ʱ����
            startFrom,            //��ʼid
            currentIndex,	     //��ǰid
            recordSixe = 0,      //ÿ�β�ѯ���ؽ��������
            maxRow;                //ÿ�β�ѯ���������


    private String table,       //Ҫ�����ı�
            columnsToSelect,     //�û�����Ĳ�ѯ����
            customQuery,          //�û�����Ĳ�ѯ���
            query,                 //�����Ĳ�ѯ���
            defaultCharsetResultSet;//���뼯

    //�����ģ�������ȡ�����ļ�
    private Context context;

    //Ϊ����ı�����ֵ��Ĭ��ֵ��������flume����������ļ����޸�
    private static final int DEFAULT_QUERY_DELAY = 10000;
    private static final int DEFAULT_START_VALUE = 0;
    private static final int DEFAULT_MAX_ROWS = 2000;
    private static final String DEFAULT_COLUMNS_SELECT = "*";
    private static final String DEFAULT_CHARSET_RESULTSET = "UTF-8";

    private static Connection conn = null;
    private static PreparedStatement ps = null;
    private static String connectionURL, connectionUserName, connectionPassword;

    //���ؾ�̬��Դ
    static {
        Properties p = new Properties();
        try {
            p.load(SQLSourceHelper.class.getClassLoader().getResourceAsStream("jdbc.properties"));
            connectionURL = p.getProperty("dbUrl");
            connectionUserName = p.getProperty("dbUser");
            connectionPassword = p.getProperty("dbPassword");
            Class.forName(p.getProperty("dbDriver"));
        } catch (IOException | ClassNotFoundException e) {
            LOG.error(e.toString());
        }
    }

    //��ȡJDBC����
    private static Connection InitConnection(String url, String user, String pw) {
        try {
            Connection conn = DriverManager.getConnection(url, user, pw);
            if (conn == null)
                throw new SQLException();
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    //���췽��
    SQLSourceHelper(Context context) throws ParseException {
        //��ʼ��������
        this.context = context;

        //��Ĭ��ֵ��������ȡflume���������ļ��еĲ������������Ĳ���Ĭ��ֵ
        this.columnsToSelect = context.getString("columns.to.select", DEFAULT_COLUMNS_SELECT);
        this.runQueryDelay = context.getInteger("run.query.delay", DEFAULT_QUERY_DELAY);
        this.startFrom = context.getInteger("start.from", DEFAULT_START_VALUE);
        this.defaultCharsetResultSet = context.getString("default.charset.resultset", DEFAULT_CHARSET_RESULTSET);

        //��Ĭ��ֵ��������ȡflume���������ļ��еĲ���
        this.table = context.getString("table");
        this.customQuery = context.getString("custom.query");
        connectionURL = context.getString("connection.url");
        connectionUserName = context.getString("connection.user");
        connectionPassword = context.getString("connection.password");
        conn = InitConnection(connectionURL, connectionUserName, connectionPassword);

        //У����Ӧ��������Ϣ�����û��Ĭ��ֵ�Ĳ���Ҳû��ֵ���׳��쳣
        checkMandatoryProperties();
        //��ȡ��ǰ��id
        currentIndex = getStatusDBIndex(startFrom);
        //������ѯ���
        query = buildQuery();
    }

    //У����Ӧ��������Ϣ������ѯ����Լ����ݿ����ӵĲ�����
    private void checkMandatoryProperties() {
        if (table == null) {
            throw new ConfigurationException("property table not set");
        }
        if (connectionURL == null) {
            throw new ConfigurationException("connection.url property not set");
        }
        if (connectionUserName == null) {
            throw new ConfigurationException("connection.user property not set");
        }
        if (connectionPassword == null) {
            throw new ConfigurationException("connection.password property not set");
        }
    }

    //����sql���
    private String buildQuery() {
        String sql = "";
        //��ȡ��ǰid
        currentIndex = getStatusDBIndex(startFrom);
        LOG.info(currentIndex + "");
        if (customQuery == null) {
            sql = "SELECT " + columnsToSelect + " FROM " + table;
        } else {
            sql = customQuery;
        }
        StringBuilder execSql = new StringBuilder(sql);
        //��id��Ϊoffset
        if (!sql.contains("where")) {
            execSql.append(" where ");
            execSql.append("id").append(">").append(currentIndex);
            return execSql.toString();
        } else {
            int length = execSql.toString().length();
            return execSql.toString().substring(0, length - String.valueOf(currentIndex).length()) + currentIndex;
        }
    }

    //ִ�в�ѯ
    List<List<Object>> executeQuery() {
        try {
            //ÿ��ִ�в�ѯʱ��Ҫ��������sql����Ϊid��ͬ
            customQuery = buildQuery();
            //��Ž���ļ���
            List<List<Object>> results = new ArrayList<>();
            if (ps == null) {
                //
                ps = conn.prepareStatement(customQuery);
            }
            ResultSet result = ps.executeQuery(customQuery);
            while (result.next()) {
                //���һ�����ݵļ��ϣ�����У�
                List<Object> row = new ArrayList<>();
                //�����ؽ�����뼯��
                for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                    row.add(result.getObject(i));
                }
                results.add(row);
            }
            LOG.info("execSql:" + customQuery + "\nresultSize:" + results.size());
            return results;
        } catch (SQLException e) {
            LOG.error(e.toString());
            // ��������
            conn = InitConnection(connectionURL, connectionUserName, connectionPassword);
        }
        return null;
    }

    //�������ת��Ϊ�ַ�����ÿһ��������һ��list���ϣ���ÿһ��С��list����ת��Ϊ�ַ���
    List<String> getAllRows(List<List<Object>> queryResult) {
        List<String> allRows = new ArrayList<>();
        if (queryResult == null || queryResult.isEmpty())
            return allRows;
        StringBuilder row = new StringBuilder();
        for (List<Object> rawRow : queryResult) {
            Object value = null;
            for (Object aRawRow : rawRow) {
                value = aRawRow;
                if (value == null) {
                    row.append(",");
                } else {
                    row.append(aRawRow.toString()).append(",");
                }
            }
            allRows.add(row.toString());
            row = new StringBuilder();
        }
        return allRows;
    }

    //����offsetԪ����״̬��ÿ�η��ؽ��������á������¼ÿ�β�ѯ��offsetֵ��Ϊ�����ж���������ʱʹ�ã���idΪoffset
    void updateOffset2DB(int size) {
        //��source_tab��ΪKEY���������������룬��������£�ÿ��Դ���Ӧһ����¼��
        String sql = "insert into flume_meta(source_tab,currentIndex) VALUES('"
                + this.table
                + "','" + (recordSixe += size)
                + "') on DUPLICATE key update source_tab=values(source_tab),currentIndex=values(currentIndex)";
        LOG.info("updateStatus Sql:" + sql);
        execSql(sql);
    }

    //ִ��sql���
    private void execSql(String sql) {
        try {
            ps = conn.prepareStatement(sql);
            LOG.info("exec::" + sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //��ȡ��ǰid��offset
    private Integer getStatusDBIndex(int startFrom) {
        //��flume_meta���в�ѯ����ǰ��id�Ƕ���
        String dbIndex = queryOne("select currentIndex from flume_meta where source_tab='" + table + "'");
        if (dbIndex != null) {
            return Integer.parseInt(dbIndex);
        }
        //���û�����ݣ���˵���ǵ�һ�β�ѯ�������ݱ��л�û�д������ݣ�������������ֵ
        return startFrom;
    }

    //��ѯһ�����ݵ�ִ�����(��ǰid)
    private String queryOne(String sql) {
        ResultSet result = null;
        try {
            ps = conn.prepareStatement(sql);
            result = ps.executeQuery();
            while (result.next()) {
                return result.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    //�ر������Դ
    void close() {
        try {
            ps.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    int getCurrentIndex() {
        return currentIndex;
    }

    void setCurrentIndex(int newValue) {
        currentIndex = newValue;
    }

    int getRunQueryDelay() {
        return runQueryDelay;
    }

    String getQuery() {
        return query;
    }

    String getConnectionURL() {
        return connectionURL;
    }

    private boolean isCustomQuerySet() {
        return (customQuery != null);
    }

    Context getContext() {
        return context;
    }

    public String getConnectionUserName() {
        return connectionUserName;
    }

    public String getConnectionPassword() {
        return connectionPassword;
    }

    String getDefaultCharsetResultSet() {
        return defaultCharsetResultSet;
    }

}
