package utils.enums;

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

    @Override
    public String toString() {
        String str = this.name();
        if (this.name().equals("_90")){
            str = str.replace("_", "") + "+";
        }
        else{
           str =  str.substring(1, 3) + "-" + str.substring(3);
        }
        return str;
    }

}
