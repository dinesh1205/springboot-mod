package com.lmig.cmit.pas.plp.ext.adapter.ofac.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.sql.DataSource;
import java.sql.Connection;

@SpringBootApplication
public class OfacProcessRecordsApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(OfacProcessRecordsApplication.class);
    private final DataSource dataSource;

    public OfacProcessRecordsApplication(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static void main(String[] args) {
        int exitCode = SpringApplication.exit(SpringApplication.run(OfacProcessRecordsApplication.class, args));
        System.exit(exitCode);
    }

    @Override
    public void run(String... args) throws Exception {
        int exitCode = 0;
        Connection conn = null;

        try {
            log.info(">>> Starting OFAC Process Records job");

            if (args.length < 4) {
                log.error("Usage: <environment> <RESENDTRUE|NULL> <FULL|DELTA> <fileDate>");
                System.exit(1);
            }

            String environment = args[0];
            boolean resend = "RESENDTRUE".equalsIgnoreCase(args[1]);
            boolean full = "FULL".equalsIgnoreCase(args[2]);
            String fileDate = args[3];

            conn = dataSource.getConnection();
            conn.setAutoCommit(false);

            OfacConnect ofacConn = new OfacConnect();
            ofacConn.initialize(environment);

            OfacDbRoutines dbRoutines = new OfacDbRoutines(conn);
            OfacResendRecordExtract orne = new OfacResendRecordExtract(resend, full, dbRoutines);
            orne.buildRecordExtract(fileDate);

            log.info("<<< OFAC Job Completed Successfully >>>");
            exitCode = 0;

        } catch (Exception e) {
            log.error("OFAC job failed", e);
            exitCode = 1;
        } finally {
            if (conn != null) conn.close();
            System.exit(exitCode);
        }
    }
}
