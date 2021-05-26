package utils.beans;

import utils.enums.AgeCategory;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class SomministrationLatest implements Serializable {
    private LocalDate date;
    private Integer female_vaccination;
    private AgeCategory ageCategory;
    private String area;

    public SomministrationLatest(String date, Integer female_vaccination, AgeCategory ageCategory, String area){
        String pattern = "yyyy-MM-dd";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        this.date = LocalDate.parse(date, formatter);
        this.female_vaccination = female_vaccination;
        this.ageCategory = ageCategory;
        this.area = area;
    }


    public LocalDate getDate() {
        return date;
    }

    public Integer getFemale_vaccination() {
        return female_vaccination;
    }

    public AgeCategory getAgeCategory() {
        return ageCategory;
    }

    public String getArea() {
        return area;
    }

    @Override
    public String toString() {
        return "SomministrationLatest{" +
                "date=" + date +
                ", female_vaccination=" + female_vaccination +
                ", ageCategory=" + ageCategory +
                ", area='" + area + '\'' +
                '}';
    }

    public static SomministrationLatest CSVParser(String csvLine) {
        SomministrationLatest summary = null;
        if (csvLine == null)
            return null;
        String[] csvValues = csvLine.split(",");
        try {
            summary = new SomministrationLatest(
                    csvValues[0],
                    Integer.valueOf(csvValues[5]),
                    AgeCategory.toEnum(csvValues[3]),
                    csvValues[21]
            );
        }catch (DateTimeParseException | NumberFormatException e){
            return  null;
        }

        return summary;

    }
}
