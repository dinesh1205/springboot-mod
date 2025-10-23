package com.lmig.cmit.pas.plp.ext.adapter.ofac.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.sql.DataSource;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;

@SpringBootApplication
public class OfacPrimerDataApplication {

    private static final Logger log = LoggerFactory.getLogger(OfacPrimerDataApplication.class);

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(OfacPrimerDataApplication.class, args)));
    }

    public CommandLineRunner runner(DataSource dataSource) {
        return args -> {
            if (args.length < 3) {
                log.error("Usage: <ENV> <filePath> <prime_data|reset_resend_date>");
                return;
            }

            String environment = args[0];
            String filePath = args[1];
            String function = args[2];

            int exitCode = 0;
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(true);
                OfacConnect ofacConn = new OfacConnect();
                ofacConn.initialize(environment);
                OfacDbRoutines dbRoutines = new OfacDbRoutines(conn);

                if ("prime_data".equalsIgnoreCase(function)) {
                    primeData(filePath, dbRoutines);
                } else if ("reset_resend_date".equalsIgnoreCase(function)) {
                    int rows = resetResendDate(dbRoutines);
                    log.info("Reset resend date, rows updated = {}", rows);
                } else {
                    log.error("Invalid function argument: {}", function);
                }

            } catch (Exception e) {
                log.error("OFAC Primer Data job failed", e);
                exitCode = 1;
            }

            SpringApplication.exit(SpringApplication.run(OfacPrimerDataApplication.class), () -> exitCode);
        };
    }

    private void primeData(String filePath, OfacDbRoutines dbRoutines) throws Exception {
        log.info("Running prime_data job, output: {}", filePath);
        String fileDate = getOfacResendDate(dbRoutines);

        StringBuilder sb = new StringBuilder();
        sb.append("ofacResendDate:").append(fileDate).append(System.lineSeparator());
        sb.append("ofacWeek:").append(getOfacResendWeek(dbRoutines)).append(System.lineSeparator());
        sb.append("ofacDay:").append(getOfacResendDay(dbRoutines)).append(System.lineSeparator());
        sb.append("ofacStandardReportWeek:").append(getOfacStandardReportWeek(dbRoutines)).append(System.lineSeparator());
        sb.append("standardisedFileDate:").append(getStandardisedFileDate(dbRoutines, fileDate.trim())).append(System.lineSeparator());

        File outputFile = new File(filePath);
        outputFile.getParentFile().mkdirs();

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
            bw.write(sb.toString());
        }

        log.info("Primer data file created successfully at {}", filePath);
    }

    // Placeholder methods delegate to dbRoutines via SQL queries (same logic as legacy).
    private String getOfacResendDate(OfacDbRoutines db) throws Exception { Object[] r = db.fetchRecords("select coalesce(VALUE,'') from COMPANYDB.LIST_OF_VALUE where LOV_NAME = 'OFAC_RESEND_DATE'"); return (r!=null && (Integer)r[1] > 0) ? r[0].toString() : ""; }
    private String getOfacResendWeek(OfacDbRoutines db) throws Exception { Object[] r = db.fetchRecords("select ceil(decfloAt(days(date(value)) - days(date('2012-12-29')))/decfloat(7)) from COMPANYDB.LIST_OF_VALUE where LOV_NAME = 'OFAC_RESEND_DATE'"); return (r!=null && (Integer)r[1] > 0) ? r[0].toString() : ""; }
    private String getOfacResendDay(OfacDbRoutines db) throws Exception { Object[] r = db.fetchRecords("select dayofweek_iso(date(value)) from COMPANYDB.LIST_OF_VALUE where LOV_NAME = 'OFAC_RESEND_DATE'"); return (r!=null && (Integer)r[1] > 0) ? r[0].toString() : ""; }
    private String getOfacStandardReportWeek(OfacDbRoutines db) throws Exception { Object[] r = db.fetchRecords("select ceil(decfloAt((days(current timestamp) - days(date('2012-12-29')))/decfloat(7))) from sysibm.sysdummy1"); return (r!=null && (Integer)r[1] > 0) ? r[0].toString() : ""; }
    private String getStandardisedFileDate(OfacDbRoutines db, String fileDate) throws Exception { Object[] r = db.fetchRecords("select char((date('" + fileDate + "') - 1 day),iso) from sysibm.sysdummy1"); return (r!=null && (Integer)r[1] > 0) ? r[0].toString() : ""; }
    private int resetResendDate(OfacDbRoutines db) throws Exception { return db.updateRecords("update COMPANYDB.LIST_OF_VALUE set VALUE=NULL where LOV_NAME = 'OFAC_RESEND_DATE'"); }
}
