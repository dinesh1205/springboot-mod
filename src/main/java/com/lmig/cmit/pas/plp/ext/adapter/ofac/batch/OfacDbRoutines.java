package com.lmig.cmit.pas.plp.ext.adapter.ofac.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class OfacDbRoutines {

    private static final Logger log = LoggerFactory.getLogger(OfacDbRoutines.class);
    private final Connection connection;

    public OfacDbRoutines(Connection connection) {
        this.connection = connection;
    }

    public void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) connection.close();
        } catch (SQLException e) {
            log.warn("Error closing DB connection", e);
        }
    }

    public int countRecords(String query) throws SQLException {
        log.debug("Executing count query: {}", query);
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    public Object[] fetchRecords(String query) throws SQLException {
        log.debug("Executing fetch query: {}", query);
        StringBuffer buffer = new StringBuffer();
        int rowCount = 0;
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                buffer.append(rs.getString(1)).append("\n");
                rowCount++;
            }
        }
        log.info("Fetched {} records", rowCount);
        return new Object[]{buffer, rowCount};
    }

    public int updateRecords(String query) throws SQLException {
        log.debug("Executing update query: {}", query);
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            int count = stmt.executeUpdate();
            log.info("Updated {} rows", count);
            return count;
        }
    }

    public String constructQuery(String fileDate, boolean resend, boolean full, boolean countRecords, boolean parentsOnly) {
        StringBuilder query = new StringBuilder();
        if (countRecords)
            query.append("SELECT COUNT(*) FROM CMEDB.OFAC_STAGE_T ");
        else
            query.append("SELECT OFAC_RCRD_TXT FROM CMEDB.OFAC_STAGE_T ");

        query.append("WHERE DATE(CNTR_EFF_DTE) <= DATE('").append(fileDate).append("') ")
             .append("AND DATE(CNTR_EXP_DTE) >= DATE('").append(fileDate).append("') ")
             .append("AND DATE(CANCN_RNSTT_DTE) IS NULL ");

        if (!full) {
            if (resend) {
                query.append("AND (DATE(OFAC_EXTR_DTE) = DATE('").append(fileDate).append("') OR EXTR_IND IS NULL) ");
            } else {
                query.append("AND EXTR_IND IS NULL ");
            }
        }

        query.append("AND DATE(ROW_CRT_DTM) <= DATE('").append(fileDate).append("') ");

        if (parentsOnly) {
            query.append("AND RCRD_NUM = '00001' ");
        }

        if (!countRecords) {
            query.append("ORDER BY CNTR_NUM, RCRD_NUM ");
        }

        query.append("WITH UR");
        log.debug("Constructed query: {}", query);
        return query.toString();
    }

    public String constructUpdateQuery(String fileDate) {
        String query = "UPDATE CMEDB.OFAC_STAGE_T SET EXTR_IND='Y', OFAC_EXTR_DTE='" + fileDate + "' " +
                "WHERE DATE(CNTR_EFF_DTE) <= DATE('" + fileDate + "') " +
                "AND DATE(CNTR_EXP_DTE) >= DATE('" + fileDate + "') " +
                "AND DATE(CANCN_RNSTT_DTE) IS NULL " +
                "AND DATE(OFAC_EXTR_DTE) IS NULL " +
                "AND EXTR_IND IS NULL " +
                "AND DATE(ROW_CRT_DTM) <= DATE('" + fileDate + "')";
        log.debug("Constructed update query: {}", query);
        return query;
    }
}
