package utils;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class SomministrationSummary implements Serializable {

    private final LocalDate somministrationDate;
    private final String area;
    private final Integer total;

    public SomministrationSummary(String somministrationDate, String area, String total) throws DateTimeParseException{
        // safe method just in case of future change of date format
        String pattern = "yyyy-MM-dd";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        this.somministrationDate = LocalDate.parse(somministrationDate, formatter);
        this.area = area;
        this.total = Integer.valueOf(total);
    }

    public LocalDate getSomministrationDate() {
        return somministrationDate;
    }

    public String getArea() {
        return area;
    }

    public Integer getTotal() {
        return total;
    }

    public static SomministrationSummary CSVParser(String csvLine) {
        SomministrationSummary summary = null;
        if (csvLine == null)
            return null;
        String[] csvValues = csvLine.split(",");
        try {
            summary = new SomministrationSummary(
                    csvValues[0],
                    csvValues[1],
                    csvValues[2]
            );
        }catch (DateTimeParseException e){
            return  null;
        }

        return summary;

    }

    @Override
    public String toString() {
        return "SomministrationSummary{" +
                "somministrationDate=" + somministrationDate +
                ", area='" + area + '\'' +
                ", total=" + total +
                '}';
    }
}
