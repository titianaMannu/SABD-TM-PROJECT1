package utils.beans;

import utils.enums.CentreType;

import java.io.Serializable;
import java.util.Objects;

public class VaccinationCenter implements Serializable {
    private final String areaCode;
    private final String areaName;
    private final String name;
    private  CentreType type;


    public VaccinationCenter(String areaCode, String areaName, String name, CentreType type) {
        this.areaCode = areaCode;
        this.areaName = areaName;
        this.name = name;
        this.type = type;

    }

    public static VaccinationCenter parse(String csvLine) {
        VaccinationCenter center;
        if (csvLine == null)
            return null;
        String[] csvValues = csvLine.split(",");

        center = new VaccinationCenter(
                csvValues[0],
                csvValues[6],
                csvValues[1],
                CentreType.value(csvValues[2])
        );

        return center;


    }

    public String getAreaCode() {
        return areaCode;
    }

    public String getAreaName() {
        return areaName;
    }

    public String getName() {
        return name;
    }

    public CentreType getType() {
        return type;
    }

    public void setType(CentreType type) {

        this.type = type;
    }

    @Override
    public String toString() {
        return "VaccinationCenter{" +
                "areaCode='" + areaCode + '\'' +
                ", areaName='" + areaName + '\'' +
                ", name='" + name + '\'' +
                ", type=" + type +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VaccinationCenter)) return false;
        VaccinationCenter that = (VaccinationCenter) o;
        return Objects.equals(this.areaCode, that.areaCode) && Objects.equals(this.name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(areaCode, name);
    }
}


