package com.lmig.cmit.pas.plp.ext.adapter.ofac.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

@Component
public class OfacResendRecordExtract {

    private static final Logger log = LoggerFactory.getLogger(OfacResendRecordExtract.class);

    private final boolean resend;
    private final boolean full;
    private final OfacDbRoutines dbRoutines;

    @Value("${ofac.output.path:/opt/app/reports}")
    private String reportBasePath;

    private String outputFilePath;
    private String backupFilePath;

    public OfacResendRecordExtract(boolean resend, boolean full, OfacDbRoutines dbRoutines) {
        this.resend = resend;
        this.full = full;
        this.dbRoutines = dbRoutines;
    }

    protected void buildRecordExtract(String fileDate) throws Exception {
        log.info("Starting OFAC record extract for fileDate: {} (resend={}, full={})", fileDate, resend, full);

        String query = dbRoutines.constructQuery(fileDate, resend, full, false, false);
        Object[] fetchResponse = dbRoutines.fetchRecords(query);
        StringBuffer buffer = (StringBuffer) fetchResponse[0];
        int totalRecords = (Integer) fetchResponse[1];
        log.info("Main query returned {} records", totalRecords);

        query = dbRoutines.constructQuery(fileDate, resend, full, true, true);
        Object[] fetchResponse2 = dbRoutines.fetchRecords(query);
        int tot01Records = (Integer) fetchResponse2[1];
        log.info("Parent query returned {} records", tot01Records);

        buffer.append(buildTrailerRecord(totalRecords, tot01Records));
        setOutputFilenames(fileDate);
        generateOutputFiles(buffer.toString());

        query = dbRoutines.constructUpdateQuery(fileDate);
        int recordsUpdated = dbRoutines.updateRecords(query);
        log.info("Total records updated: {}", recordsUpdated);
    }

    protected void setOutputFilenames(String fileDate) {
        if (full) {
            outputFilePath = reportBasePath + "/OFAC_Biweekly_Records.txt";
            backupFilePath = reportBasePath + "/OFAC_Biweekly_Records_" + fileDate + ".txt";
        } else {
            outputFilePath = reportBasePath + "/OFAC_Records.txt";
            backupFilePath = reportBasePath + "/OFAC_Records_" + fileDate + ".txt";
        }
    }

    protected void generateOutputFiles(String bufferData) throws IOException {
        try (BufferedWriter writer1 = new BufferedWriter(new FileWriter(outputFilePath));
             BufferedWriter writer2 = new BufferedWriter(new FileWriter(backupFilePath))) {

            writer1.write(bufferData);
            writer2.write(bufferData);
            log.info("Wrote OFAC extract to {} and {}", outputFilePath, backupFilePath);
        }
    }

    protected String buildTrailerRecord(int totRec, int tot01Rec) {
        StringBuilder trailer = new StringBuilder();
        trailer.append("TLR");
        trailer.append("Clipper IF");
        trailer.append(new java.text.SimpleDateFormat("MMddyyyyHHmmss").format(new java.util.Date()));
        trailer.append(bufferBuilder(tot01Rec, 9));
        trailer.append(bufferBuilder(totRec, 9));
        trailer.append(bufferBuilder("503", 9));
        trailer.append("\n");
        return trailer.toString();
    }

    protected String bufferBuilder(int field, int fixedFieldLength) {
        return String.format("%0" + fixedFieldLength + "d", field);
    }

    protected String bufferBuilder(String field, int fixedFieldLength) {
        if (field == null) field = "";
        if (field.length() > fixedFieldLength) {
            field = field.substring(0, fixedFieldLength);
        }
        return String.format("%-" + fixedFieldLength + "s", field);
    }
}
