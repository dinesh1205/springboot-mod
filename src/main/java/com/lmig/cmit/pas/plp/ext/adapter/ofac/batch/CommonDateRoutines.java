package com.lmig.cmit.pas.plp.ext.adapter.ofac.batch;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class CommonDateRoutines {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static String fetchDateNewFormat(int numberOfDays) {
        LocalDate resultDate = LocalDate.now().plusDays(numberOfDays);
        return resultDate.format(FORMATTER);
    }
}
