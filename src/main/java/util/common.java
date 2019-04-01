package util;

public class common {
    public static long parseSize(String arg) {
        String[] args = arg.split("\\D", 2);
        assert args.length <= 2;
        //System.out.println("hello"+args[0]);
        long nrBytes = Integer.parseInt(args[0]);
        String bytesMult = arg.substring(args[0].length());
        return nrBytes * ByteMultiple.parseString(bytesMult).value();
    }

    static enum ByteMultiple {
        B(1L),
        KB(1024L),
        MB(1048576L),
        GB(1073741824L),
        TB(1099511627776L);

        private long multiplier;

        private ByteMultiple(long mult) {
            this.multiplier = mult;
        }

        long value() {
            return this.multiplier;
        }

        static ByteMultiple parseString(String sMultiple) {
            if(sMultiple != null && sMultiple.length()>0) {
                String sMU = sMultiple.toUpperCase();
                if(B.name().toUpperCase().endsWith(sMU)) {
                    return B;
                } else if(KB.name().toUpperCase().endsWith(sMU)) {
                    return KB;
                } else if(MB.name().toUpperCase().endsWith(sMU)) {
                    return MB;
                } else if(GB.name().toUpperCase().endsWith(sMU)) {
                    return GB;
                } else if(TB.name().toUpperCase().endsWith(sMU)) {
                    return TB;
                } else {
                    throw new IllegalArgumentException("Unsupported ByteMultiple " + sMultiple);
                }
            } else {
                return MB;
            }
        }
    }
}
