package com.lmig.cmit.pas.plp.ext.adapter.ofac.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.sql.DataSource;
import java.sql.Connection;

@SpringBootApplication
public class OfacProcessRecordsApplication {

    private static final Logger log = LoggerFactory.getLogger(OfacProcessRecordsApplication.class);

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(OfacProcessRecordsApplication.class, args)));
    }

    public CommandLineRunner runner(DataSource dataSource) {
        return args -> {
            int exitCode = 0;
            try (Connection conn = dataSource.getConnection()) {
                log.info(">>> Starting OFAC Process Records job");
                if (args.length < 4) {
                    log.error("Usage: <environment> <RESENDTRUE|NULL> <FULL|DELTA> <fileDate>");
                    return;
                }

                String environment = args[0];
                boolean resend = "RESENDTRUE".equalsIgnoreCase(args[1]);
                boolean full = "FULL".equalsIgnoreCase(args[2]);
                String fileDate = args[3];

                OfacConnect ofacConn = new OfacConnect();
                ofacConn.initialize(environment);

                OfacDbRoutines dbRoutines = new OfacDbRoutines(conn);
                OfacResendRecordExtract orne = new OfacResendRecordExtract(resend, full, dbRoutines);
                orne.buildRecordExtract(fileDate);

                log.info("<<< OFAC Job Completed Successfully >>>");

            } catch (Exception e) {
                log.error("OFAC job failed", e);
                exitCode = 1;
            } finally {
                SpringApplication.exit(SpringApplication.run(OfacProcessRecordsApplication.class), () -> exitCode);
            }
        };
    }
}
