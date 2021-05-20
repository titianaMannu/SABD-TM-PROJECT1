package utils;

enum CentreType {
    HOSPITAL, TERRITORIAL;

    public static CentreType value(String str) throws IllegalArgumentException {
        if (str.equals("Ospedaliero")) {
            return HOSPITAL;
        }else {
            return TERRITORIAL;
        }
    }

}
