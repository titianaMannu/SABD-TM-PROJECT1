package utils;

public enum AgeCategory {
    _1619, _2029, _3039, _4049, _5059, _6069, _7079, _8089, _90;

    public static AgeCategory toEnum(String str){
        str = str.replace("-", "").replace("+", "");
        str = "_" + str;
        try {
            return AgeCategory.valueOf(str);
        }catch (IllegalArgumentException ill){
            return null;
        }
    }
}
