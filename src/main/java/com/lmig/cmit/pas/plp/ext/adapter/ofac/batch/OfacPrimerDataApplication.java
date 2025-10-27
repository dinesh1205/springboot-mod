package com.lmig.cmit.pas.plp.ext.adapter.ofac.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.sql.DataSource;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * OFAC Primer Data Batch Job
 * ---------------------------
 * This job fetches resend-related information (resend date, week, etc.)
 * and writes it to a text file. Optionally, it resets resend flags in the DB.
 */
@SpringBootApplication
public class OfacPrimerDataApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(OfacPrimerDataApplication.class);
    private final DataSource dataSource;

    public OfacPrimerDataApplication(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static void main(String[] args) {
        int exitCode = SpringApplication.exit(SpringApplication.run(OfacPrimerDataApplication.class, args));
        System.exit(exitCode);
    }

    @Override
    public void run(String... args) throws Exception {
        int exitCode = 0;
        Connection conn = null;

        try {
            log.info(">>> Starting OFAC Primer Data job");

            if (args.length < 2) {
                log.error("Usage: <environment> <RESETFLAG(TRUE|FALSE)>");
                System.exit(1);
            }

            String environment = args[0];
            boolean resetFlag = "TRUE".equalsIgnoreCase(args[1]);

            conn = dataSource.getConnection();
            conn.setAutoCommit(false);

            OfacConnect ofacConn = new OfacConnect();
            ofacConn.initialize(environment);

            log.info("Connected to environment: {}", environment);

            String outputFile = "./output/ofac_primer_data.txt";

            // 1️⃣ Fetch resend date and week info
            fetchPrimerData(conn, outputFile);

            // 2️⃣ Reset resend date if requested
            if (resetFlag) {
                resetResendDate(conn);
            }

            conn.commit();
            log.info("<<< OFAC Primer Data Job Completed Successfully >>>");
            exitCode = 0;

        } catch (Exception e) {
            log.error("OFAC Primer Data job failed", e);
            if (conn != null) conn.rollback();
            exitCode = 1;

        } finally {
            if (conn != null) conn.close();
            System.exit(exitCode);
        }
    }

    /**
     * Fetches resend-related information from the database
     * and writes it to an output text file.
     */
    private void fetchPrimerData(Connection conn, String outputFile) throws Exception {
        String query = "SELECT RESEND_DATE, REPORT_WEEK, REPORT_DAY " +
                "FROM CMEDB.OFAC_PRIMER_CONTROL WHERE STATUS = 'ACTIVE'";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query);
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {

            writer.write("RESEND_DATE,REPORT_WEEK,REPORT_DAY");
            writer.newLine();

            while (rs.next()) {
                String line = rs.getString("RESEND_DATE") + "," +
                        rs.getString("REPORT_WEEK") + "," +
                        rs.getString("REPORT_DAY");
                writer.write(line);
                writer.newLine();
            }

            log.info("Primer data written to {}", outputFile);
        }
    }

    /**
     * Resets resend date and flags in the control table.
     */
    private void resetResendDate(Connection conn) throws Exception {
        String updateQuery = "UPDATE CMEDB.OFAC_PRIMER_CONTROL " +
                "SET RESEND_DATE = NULL, STATUS = 'READY' WHERE STATUS = 'ACTIVE'";

        try (Statement stmt = conn.createStatement()) {
            int updated = stmt.executeUpdate(updateQuery);
            log.info("Reset resend date for {} record(s)", updated);
        }
    }
}
