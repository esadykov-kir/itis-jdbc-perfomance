package ru.kirkazan.itis.jdbc.perfomance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class Main {

    private static Logger logger = LoggerFactory.getLogger("Main");
    private static Connection connection;

    private static void fill() {

        Statement st = null;

        try {
            st = connection.createStatement();

            for (int i = 0; i < 1000000; i++) {
                int inserted = st.executeUpdate(
                        "insert into test values ("+ String.valueOf(i) + " , " + String.valueOf(i) + ")");

                //logger.info("Inserted {} row(s).", inserted);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (st != null && !st.isClosed())
                    st.close();
            } catch (SQLException e) {
                logger.error("Error on fill st.close()", e);
                System.exit(-4);
            }
        }

    }



    public static void main(String[] args) {
        init();
        printSize();

        long start = System.currentTimeMillis();

        fill();

        logger.info("Filling in {} ms.", System.currentTimeMillis() - start);
        printSize();
        destroy();

    }

    private static void init() {
        try {
            connection = DriverManager.getConnection("jdbc:hsqldb:file:testdb");
        } catch (SQLException e) {
            logger.error("Error on connection", e);
            System.exit(-1);
        }
        try {
            Statement st = connection.createStatement();
            st.execute("CREATE TABLE test (id integer, value integer);");
            st.close();
        } catch (SQLException e) {
            logger.error("Error on create table", e);
            System.exit(-6);
        }
    }

    private static void destroy() {

        try {
            Statement st = connection.createStatement();
            st.execute("DROP TABLE test;");
            st.close();
        } catch (SQLException e) {
            logger.error("Error on drop table", e);
            System.exit(-5);
        }
        try {
            if (connection != null && !connection.isClosed())
                connection.close();
        } catch (SQLException e) {
            logger.error("Error on connection.close()", e);
            System.exit(-2);
        }
    }

    private static void printSize() {
        Statement st = null;
        ResultSet rs = null;

        try {
            st = connection.createStatement();
            rs = st.executeQuery("select count(*) from test");
            rs.next();
            logger.info("Current table size is {} rows.", rs.getInt(1));
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (st != null && !st.isClosed())
                    st.close();
            } catch (SQLException e) {
                logger.error("Error on printSize st.close()", e);
                System.exit(-3);
            }
        }

    }

}
