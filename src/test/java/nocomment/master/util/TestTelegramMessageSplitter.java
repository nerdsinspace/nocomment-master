package nocomment.master.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;

public class TestTelegramMessageSplitter {
    private static String withoutChar(String str, char filterChar) {
        return str.chars()
            .filter(c -> c != filterChar)
            .collect(StringBuilder::new, (sb, c) -> sb.append((char) c), StringBuilder::append)
            .toString();
    }

    @Test
    public void testSplitter() {
        Random rand = new Random();
        final String randomString = rand
            .ints(1_000_000, 0, 255)
            .collect(StringBuilder::new, (sb, i) -> sb.append((char) i), StringBuilder::append)
            .toString();

        final int MAX_LINE_LENGTH = 4000;
        final List<String> split = Telegram.splitIntoMultipleBigMessages(randomString, MAX_LINE_LENGTH);
        final String recombined = String.join("", split);

        // Will never get a line longer than the max
        Assert.assertTrue(split.stream().noneMatch(line -> line.length() > MAX_LINE_LENGTH));
        // The original string can't be fully reconstructed but ignoring new lines they will be the same
        Assert.assertEquals(withoutChar(randomString, '\n'), withoutChar(recombined, '\n'));
    }
}
