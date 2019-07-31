package com.youfan.bigdata.hbase.format;

import com.youfan.bigdata.hbase.bean.CacheData;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Create by chenqinping on 2019/4/8 18:43
 */
public class MysqlOutputFormat extends OutputFormat<Text, CacheData> {

    class MysqlRecordWriter extends RecordWriter<Text, CacheData> {
        private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
        private static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306/ct?useUnicode=true&characterEncoding=UTF-8";
        private static final String MYSQL_USERNAME = "root";
        private static final String MYSQL_PASSWORD = "000000";

        private Connection connection;

        public MysqlRecordWriter() {
            try {
                Class.forName(MYSQL_DRIVER_CLASS);
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }


        @Override
        public void write(Text key, CacheData value) throws IOException, InterruptedException {

            String sql = "insert into statresult (name,sumcount) values(?,?)";
            PreparedStatement preparedStatement = null;
            try {
                preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setObject(1, key.toString());
                preparedStatement.setObject(2, value.getCount());
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
    }


    @Override
    public RecordWriter<Text, CacheData> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MysqlRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }


    private FileOutputCommitter committer = null;

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }
}
