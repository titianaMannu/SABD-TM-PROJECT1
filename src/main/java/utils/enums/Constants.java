package utils.enums;

public enum Constants {
    PATHQ1_CENTRI("data/punti-somministrazione-tipologia.csv"),
    PATHQ1_SUMMARY("data/somministrazioni-vaccini-summary-latest.csv"),
    PATHQ2_LATEST("data/somministrazioni-vaccini-latest.csv"),
    OUTPUT_PATH_Q1("Results/Query1"),
    OUTPUT_PATH_Q2("Results/Query2"),
    MASTER_URL("local"),
    Q1_SCHEMA( "mese,regione,media_giornaliera_per_centro"),
    Q2_SCHEMA("mese,fascia età,regione,valore predetto");

    private final String string;

    Constants(String first) {
        this.string = first;
    }

    public String getString() {
        return string;
    }
}
