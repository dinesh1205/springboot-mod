package com.lmig.cmit.pas.plp.ext.adapter.ofac.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class OfacConnect {

    private static final Logger log = LoggerFactory.getLogger(OfacConnect.class);

    private String dbURL;
    private String user;
    private String password;
    private String dbDriver;
    private String decryptionKeyClass;

    public void initialize(String environment) throws Exception {
        Properties props = new Properties();
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("OFACBatch.properties")) {
            if (in != null) {
                props.load(in);
                log.info("Loaded OFACBatch.properties from classpath");
            } else {
                log.info("OFACBatch.properties not found on classpath — will use environment variables if present");
            }
        }

        dbURL = System.getenv().getOrDefault("DB_URL", props.getProperty(environment + ".database.url", ""));
        user = System.getenv().getOrDefault("DB_USER", props.getProperty(environment + ".database.user", ""));
        password = System.getenv().getOrDefault("DB_PASS", props.getProperty(environment + ".database.password", ""));
        decryptionKeyClass = props.getProperty(environment + ".database.decryptKey", "");
        dbDriver = props.getProperty("db.driver", "com.ibm.db2.jcc.DB2Driver");
    }

    public String getDbURL() { return dbURL; }
    public String getUser() { return user; }
    public String getPassword() { return password; }
    public String getDbDriver() { return dbDriver; }
}
