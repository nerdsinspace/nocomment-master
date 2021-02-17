package nocomment.master.util;

import nocomment.master.NoComment;

public class Config {
    public static void checkSafety() {
        if (getRuntimeVariable("SAFETY", "false").equals("true") && !NoComment.DRY_RUN) {
            throw new IllegalStateException("Safety!");
        }
    }

    public static String getRuntimeVariable(final String key, final String defaultValue) {
        String value = System.getenv(key);
        if (value == null) {
            value = System.getProperty(key, defaultValue);
        }
        return value;
    }
}
