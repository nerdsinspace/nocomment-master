package nocomment.master.util;

import com.pengrad.telegrambot.TelegramBot;
import com.pengrad.telegrambot.request.SendMessage;
import nocomment.master.NoComment;

import java.io.PrintWriter;
import java.io.StringWriter;

public enum Telegram {
    INSTANCE;
    private final TelegramBot bot = new TelegramBot(NoComment.getRuntimeVariable("TELEGRAM_BOT_TOKEN", ""));
    private final String chatID = NoComment.getRuntimeVariable("TELEGRAM_CHAT_ID", "");

    public void startup() {
        bot.execute(new SendMessage(chatID, "Master startup"));
    }

    public void complain(Throwable th) {
        StringWriter errors = new StringWriter();
        th.printStackTrace(new PrintWriter(errors));
        for (String line : errors.toString().split("\n")) {
            bot.execute(new SendMessage(chatID, line));
        }
    }
}
