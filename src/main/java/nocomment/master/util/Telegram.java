package nocomment.master.util;

import com.pengrad.telegrambot.TelegramBot;
import com.pengrad.telegrambot.request.SendMessage;
import nocomment.master.NoComment;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public enum Telegram {
    INSTANCE;
    private final TelegramBot bot = new TelegramBot(Config.getRuntimeVariable("TELEGRAM_BOT_TOKEN", ""));
    private final String chatID = Config.getRuntimeVariable("TELEGRAM_CHAT_ID", "");

    public void startup() {
        if (NoComment.DRY_RUN) {
            bot.execute(new SendMessage(chatID, "DRY RUN startup of master, probably dev env!"));
        } else {
            bot.execute(new SendMessage(chatID, "Master startup"));
        }
    }

    static int indexOfLeft(String str, char c, int from) {
        for (int i = from; i >= 0; i--) {
            if (str.charAt(i) == c) return i;
        }
        return -1;
    }

    static List<String> splitIntoMultipleBigMessages(String str, int maxSize) {
        if (str.length() <= maxSize) {
            return Collections.singletonList(str);
        } else {
            final int lastNewLine = indexOfLeft(str, '\n', maxSize - 1);
            final int leftSplit;
            final int rightSplit;
            if (lastNewLine == -1) {
                // troll stacktrace how
                leftSplit = maxSize;
                rightSplit = maxSize;
            } else {
                // don't include the last newline
                leftSplit = lastNewLine;
                rightSplit = lastNewLine + 1;
            }

            final List<String> out = new ArrayList<>();
            final String left = str.substring(0, leftSplit);
            final String right = str.substring(rightSplit);
            out.add(left);
            out.addAll(splitIntoMultipleBigMessages(right, maxSize));
            return out;
        }
    }

    public void complain(Throwable th) {
        StringWriter errors = new StringWriter();
        th.printStackTrace(new PrintWriter(errors));
        sendMessage(errors.toString());
    }

    public void sendMessage(String msg) {
        for (String line : splitIntoMultipleBigMessages(msg, 4000)) {
            bot.execute(new SendMessage(chatID, line));
        }
    }
}
