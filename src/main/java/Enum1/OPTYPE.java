package Enum1;

public enum OPTYPE {
    WRITE("write"),
    READ("read");
    private String type;
    OPTYPE(String t) {
        this.type = t;
    }
}
