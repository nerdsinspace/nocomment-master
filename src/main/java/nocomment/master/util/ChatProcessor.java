package nocomment.master.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import nocomment.master.db.Database;
import nocomment.master.tracking.TrackyTrackyManager;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum ChatProcessor {
    INSTANCE;
// delete from chat_death; delete from chat_whisper; delete from chat_server_message; delete from chat_player_message; delete from chat_miscellaneous; update chat_progress set max_created_at_processed=0;

    private static final long DEDUPLICATE_WITHIN = TimeUnit.SECONDS.toMillis(30); // because minecraft times out after 30 seconds of lag, its implausible to imagine chat lagging for more than 30 seconds
    private static final int MAX_ROWS = 10000;
    private static final int CHAT_TYPES = 3;

    public void beginIncrementalChatProcessorThread() {
        TrackyTrackyManager.scheduler.scheduleWithFixedDelay(LoggingExecutor.wrap(this::loop), 0, 5, TimeUnit.MINUTES);
    }

    public void loop() {
        try {
            while (run()) ;
        } catch (Throwable th) {
            Telegram.INSTANCE.complain(th);
        }
    }

    private static class RawChatRow {

        private final String chatRaw;
        private final int chatKey;
        private final short chatType;
        private final int reportedBy;
        private final long createdAt;
        private final short serverID;
        private final int equalityKey;

        private ParsedChatRow asParsed;

        private RawChatRow(ResultSet rs, JsonCache cache) throws SQLException {
            this.chatRaw = rs.getString("data");
            this.chatType = rs.getShort("chat_type");
            this.chatKey = cache.enroll(chatRaw, chatType);
            this.reportedBy = rs.getInt("reported_by");
            this.createdAt = rs.getLong("created_at");
            this.serverID = rs.getShort("server_id");
            this.equalityKey = (serverID * MAX_ROWS + chatKey) * CHAT_TYPES + chatType;
        }
    }

    private static abstract class ParsedChatRow {

        public abstract boolean deduplicateBetweenPlayers();

        public void save(Connection connection, RawChatRow context) throws SQLException {
            if (deduplicateBetweenPlayers()) {
                throw new IllegalStateException("chat types that are deduplicated between players must NOT go into chat miscellaneous table");
            }
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO chat_miscellaneous (created_at, server_id, reported_by, data, kind, extracted, chat_type) VALUES (?, ?, ?, CAST(? AS JSONB), ?, ?, ?)")) {
                populateWithReportedBy(stmt, context);
                stmt.setString(4, context.chatRaw);
                stmt.setString(5, getClass().getSimpleName());
                stmt.setString(6, extracted());
                stmt.setShort(7, context.chatType);
                stmt.execute();
            }
        }

        public String extracted() {
            throw new UnsupportedOperationException();
        }

        public void populateBasicData(PreparedStatement stmt, RawChatRow context) throws SQLException {
            stmt.setLong(1, context.createdAt);
            stmt.setShort(2, context.serverID);
        }

        public void populateWithReportedBy(PreparedStatement stmt, RawChatRow context) throws SQLException {
            populateBasicData(stmt, context);
            stmt.setInt(3, context.reportedBy);
        }
    }

    private static class GameInfo extends ParsedChatRow {

        private final String translationKey;

        private GameInfo(String translationKey) {
            this.translationKey = translationKey;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public String extracted() {
            return translationKey;
        }
    }

    private static class ServerMessage extends ParsedChatRow {

        private final String msg;

        private ServerMessage(String msg) {
            this.msg = msg;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return true;
        }

        @Override
        public void save(Connection connection, RawChatRow context) throws SQLException {
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO chat_server_message (created_at, server_id, message) VALUES (?, ?, ?)")) {
                populateBasicData(stmt, context);
                stmt.setString(3, msg);
                stmt.execute();
            }
        }
    }

    private static class ShutdownLeftTheGame extends ParsedChatRow { // color yellow

        private final String msg;

        private ShutdownLeftTheGame(String msg) {
            this.msg = msg;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return true;
        }

        @Override
        public void save(Connection connection, RawChatRow context) {
            // "left the game" doesn't matter; player_sessions already has it
        }
    }

    private static class NormalLeftTheGame extends ParsedChatRow { // color gray

        private final String msg;

        private NormalLeftTheGame(String msg) {
            this.msg = msg;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return true;
        }

        @Override
        public void save(Connection connection, RawChatRow context) {
            // "left the game" doesn't matter; player_sessions already has it
        }
    }

    private static class NormalJoined extends ParsedChatRow { // color gray

        private final String msg;

        private NormalJoined(String msg) {
            this.msg = msg;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return true;
        }

        @Override
        public void save(Connection connection, RawChatRow context) {
            // "joined the game" doesn't matter; player_sessions already has it
        }
    }

    private static class QueuePosition extends ParsedChatRow {

        private final int pos;

        private QueuePosition(int pos) {
            this.pos = pos;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public String extracted() {
            return Integer.toString(pos);
        }
    }


    private static class GoldCommandResponse extends ParsedChatRow {

        private static final Set<String> OPTIONS = Collections.unmodifiableSet(new HashSet<String>() {{
            add("Chat messages unhidden.");
            add("Chat messages hidden.");
            add("Death messages unhidden.");
            add("Death messages hidden.");
            add("Connection messages unhidden.");
            add("Connection messages hidden.");
            add("Private messages unhidden.");
            add("Private messages hidden.");
            add("You have nobody to reply to.");
            add("This player is not online.");
            add("-----------------------------------------------------");
            add("No players ignored.");
            add("You have toggled off chat");
        }});

        private final String msg;

        private GoldCommandResponse(String msg) {
            if (!OPTIONS.contains(msg)) {
                throw new IllegalArgumentException("unrecognized gold command response");
            }
            this.msg = msg;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public String extracted() {
            return msg;
        }
    }

    private static class GoldPluginMessage extends ParsedChatRow {

        private static final Set<String> OPTIONS = Collections.unmodifiableSet(new HashSet<String>() {{
            add("Connecting to the server...");
            add("You have lost connection to the server");
        }});

        private final String msg;

        private GoldPluginMessage(String msg) {
            if (!OPTIONS.contains(msg)) {
                throw new IllegalArgumentException("unrecognized gold plugin message");
            }
            this.msg = msg;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public String extracted() {
            return msg;
        }
    }

    private static class DarkRedCommandResponse extends ParsedChatRow {

        private static final Set<String> OPTIONS = Collections.unmodifiableSet(new HashSet<String>() {{
            add("Bad command. Type /help for all commands.");
        }});

        private final String msg;

        private DarkRedCommandResponse(String msg) {
            if (!OPTIONS.contains(msg)) {
                throw new IllegalArgumentException("unrecognized dark red command response");
            }
            this.msg = msg;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public String extracted() {
            return msg;
        }
    }

    private static class DarkRedPluginMessage extends ParsedChatRow {

        private static final Set<String> OPTIONS = Collections.unmodifiableSet(new HashSet<String>() {{
            add("Bad command. Type /guide or /help.");
        }});

        private final String msg;

        private DarkRedPluginMessage(String msg) {
            if (!OPTIONS.contains(msg)) {
                throw new IllegalArgumentException("unrecognized dark red plugin message");
            }
            this.msg = msg;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public String extracted() {
            return msg;
        }
    }

    private static class DarkAquaCommandResponse extends ParsedChatRow {

        private static final Set<String> OPTIONS = Collections.unmodifiableSet(new HashSet<String>() {{
            add("world is 10 years old and 9567 GB.");
            add("world is 10 years and 3 months old, and 10 313 GB.");
            add("603,244 players have spawned at least once on the world.");
            add("639,377 players have spawned at least once on the world.");
            add("/r or /reply to reply to the last person that messaged you.");
            add("/toggledeathmsgs to toggle all death messages.");
            add("/ignore <name> to ignore a player temporarily.");
            add("/stats for world age, size and player files saved.");
            add("/ignorelist to list ignored players and unignore.");
            add("/ignoredeathmsgs <name> to ignore a players death messages.");
            add("/t <name> <message> OR /w <name> <message> to pm a player.");
            add("/l or /last to send a message the last person you messaged.");
            add("/toggleprivatemsgs to toggle private messages.");
            add("/toggleconnectionmsgs to toggle player connection messages.");
            add("/ignorehard <name> to ignore a player permanently.");
            add("/togglechat to toggle the default chat.");
        }});

        private final String msg;

        private DarkAquaCommandResponse(String msg) {
            if (!OPTIONS.contains(msg)) {
                throw new IllegalArgumentException("unrecognized dark red command response");
            }
            this.msg = msg;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public String extracted() {
            return msg;
        }
    }

    private static class EmptyMessage extends ParsedChatRow {

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public String extracted() {
            return "";
        }
    }

    private static class ListOfIgnoredPlayers extends ParsedChatRow {

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public String extracted() {
            return "";
        }
    }

    private static class PlayerChat extends ParsedChatRow {

        private final String author;
        private final String msg;

        private PlayerChat(String author, String msg) {
            this.author = author;
            this.msg = msg;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return true;
        }

        @Override
        public void save(Connection connection, RawChatRow context) throws SQLException {
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO chat_player_message (created_at, server_id, message, author) VALUES (?, ?, ?, ?)")) {
                populateBasicData(stmt, context);
                stmt.setString(3, msg);
                stmt.setString(4, author);
                stmt.execute();
            }
        }
    }

    private static class Whisper extends ParsedChatRow {

        private final String otherPlayer;
        private final String msg;
        private final boolean outgoing;

        private Whisper(String otherPlayer, String msg, boolean outgoing) {
            this.otherPlayer = otherPlayer;
            this.msg = msg;
            this.outgoing = outgoing;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public void save(Connection connection, RawChatRow context) throws SQLException {
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO chat_whisper (created_at, server_id, reported_by, message, other_player, outgoing) VALUES (?, ?, ?, ?, ?, ?)")) {
                populateWithReportedBy(stmt, context);
                stmt.setString(4, msg);
                stmt.setString(5, otherPlayer);
                stmt.setBoolean(6, outgoing);
                stmt.execute();
            }
        }
    }

    private static class Ignore extends ParsedChatRow {

        private final String player;

        private Ignore(String player) {
            this.player = player;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public String extracted() {
            return player;
        }
    }

    private static class IgnoreHard extends ParsedChatRow {

        private final String player;

        private IgnoreHard(String player) {
            this.player = player;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public String extracted() {
            return player;
        }
    }

    private static class UnIgnore extends ParsedChatRow {

        private final String player;

        private UnIgnore(String player) {
            this.player = player;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public String extracted() {
            return player;
        }
    }

    private static class UnIgnoreHard extends ParsedChatRow {

        private final String player;

        private UnIgnoreHard(String player) {
            this.player = player;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return false;
        }

        @Override
        public String extracted() {
            return player;
        }
    }

    private static class Death extends ParsedChatRow {

        private final String template;
        private final String player1;
        private final String player2;

        private Death(String template, String player1, String player2) {
            Objects.requireNonNull(player1);
            this.template = template;
            this.player1 = player1;
            this.player2 = player2;
        }

        @Override
        public boolean deduplicateBetweenPlayers() {
            return true;
        }

        @Override
        public void save(Connection connection, RawChatRow context) throws SQLException {
            try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO chat_death (created_at, server_id, template, player_1, player_2) VALUES (?, ?, ?, ?, ?)")) {
                populateBasicData(stmt, context);
                stmt.setString(3, template);
                stmt.setString(4, player1);
                if (player2 == null) {
                    stmt.setNull(5, Types.VARCHAR);
                } else {
                    stmt.setString(5, player2);
                }
                stmt.execute();
            }
        }
    }

    private static void sanity(JsonObject json) {
        if (json.has("extra") && json.has("text") && !json.get("text").getAsString().isEmpty()) {
            throw new IllegalStateException("Wtf " + json);
        }
    }

    private static final Pattern NORMAL_CHAT = Pattern.compile("<([a-zA-Z0-9_]{1,16})> (.+)");

    private static String trim(String text, String prefix, String suffix) {
        if (!text.startsWith(prefix) || !text.endsWith(suffix)) {
            throw new IllegalStateException("bad format");
        }
        return text.substring(prefix.length(), text.length() - suffix.length());
    }

    private static ParsedChatRow parseChatRow(JsonObject json, int chatType) {
        if (chatType < 0 || chatType > 2) {
            throw new IllegalStateException();
        }
        if (chatType == 2) {
            if (!json.has("translate")) {
                throw new IllegalStateException("no translation key");
            }
            if (json.size() > 1 && !json.toString().equals("{\"color\":\"red\",\"translate\":\"build.tooHigh\",\"with\":[\"256\"]}")) {
                throw new IllegalStateException("too big");
            }
            return new GameInfo(json.get("translate").getAsString());
        }
        if (chatType == 1 && json.toString().equals("{\"text\":\"\"}")) {
            return new EmptyMessage();
        }
        if (chatType == 1 && json.toString().equals("{\"extra\":[{\"text\":\"<TullyBoob> \"},{\"clickEvent\":{\"action\":\"open_url\",\"value\":\"http://2b2tshop.net\"},\"text\":\"2b2tshop.net\"}],\"text\":\"\"}")) {
            // i can't fucking believe this happened
            return new PlayerChat("TullyBoob", "2b2tshop.net");
        }
        sanity(json);

        if (json.size() != 2 || !json.has("text")) {
            throw new IllegalStateException();
        }
        if (json.has("color")) {
            String text = json.get("text").getAsString();
            String color = json.get("color").getAsString();
            if (color.equals("gold")) {
                return new GoldPluginMessage(text);
            }
            if (color.equals("dark_red")) {
                return new DarkRedPluginMessage(text);
            }
            throw new IllegalStateException("unknown raw");
        }

        JsonArray elems = json.get("extra").getAsJsonArray();
        if (chatType == 1) {
            if (elems.size() != 1) {
                if (elems.size() == 2) {
                    JsonObject secondPart = elems.get(1).getAsJsonObject();
                    if (elems.get(0).toString().equals("{\"color\":\"gold\",\"text\":\"Position in queue: \"}")) {
                        if (secondPart.size() != 3 || !secondPart.get("bold").getAsBoolean() || !secondPart.get("color").getAsString().equals("gold")) {
                            throw new IllegalStateException("bad queue");
                        }
                        return new QueuePosition(Integer.parseInt(secondPart.get("text").getAsString()));
                    }
                    if (elems.get(0).toString().equals("{\"color\":\"gold\",\"text\":\"No longer permanently ignoring \"}")) {
                        if (secondPart.size() != 2 || !secondPart.get("color").getAsString().equals("dark_aqua")) {
                            throw new IllegalStateException("bad npi");
                        }
                        return new UnIgnoreHard(secondPart.get("text").getAsString());
                    }
                    if (elems.get(0).toString().equals("{\"color\":\"gold\",\"text\":\"No longer ignoring \"}")) {
                        if (secondPart.size() != 2 || !secondPart.get("color").getAsString().equals("dark_aqua")) {
                            throw new IllegalStateException("bad ni");
                        }
                        return new UnIgnore(trim(secondPart.get("text").getAsString(), "", ".")); // of course, unignore should put a period after the player name, but unignorehard shouldn't
                    }
                    if (elems.get(0).toString().equals("{\"color\":\"gold\",\"text\":\"Now ignoring \"}")) {
                        if (secondPart.size() != 2 || !secondPart.get("color").getAsString().equals("dark_aqua")) {
                            throw new IllegalStateException("bad i");
                        }
                        return new Ignore(secondPart.get("text").getAsString());
                    }
                    if (secondPart.toString().equals("{\"color\":\"dark_red\",\"text\":\"/ignorelist.\"}")) {
                        JsonObject firstPart = elems.get(0).getAsJsonObject();
                        if (firstPart.size() != 2 || !firstPart.get("color").getAsString().equals("gold")) {
                            throw new IllegalStateException("bad pi");
                        }
                        return new IgnoreHard(trim(firstPart.get("text").getAsString(), "Permanently ignoring ", ". This is saved in "));
                    }
                }
                throw new IllegalStateException("elems wrong size");
            }
            JsonObject msg = elems.get(0).getAsJsonObject();
            sanity(msg);
            String text = msg.get("text").getAsString();
            if (msg.size() == 1) {
                Matcher m = NORMAL_CHAT.matcher(text);
                if (!m.find()) {
                    throw new IllegalStateException("not normal chat");
                }
                return new PlayerChat(m.group(1), m.group(2)); // last message before server shutdown, chat decorator plugin async deloaded (haha d_loaded)
            }
            if (msg.size() != 2) {
                throw new IllegalStateException("expected 2");
            }
            String color = msg.get("color").getAsString();
            if (color.equals("yellow")) {
                String serverPrefix = "[SERVER] ";
                if (text.startsWith(serverPrefix)) {
                    return new ServerMessage(text.substring(serverPrefix.length()));
                }
                String leftGame = " left the game";
                if (text.endsWith(leftGame)) {
                    return new ShutdownLeftTheGame(text.substring(0, text.length() - leftGame.length()));
                }
                throw new IllegalStateException("bad yellow");
            }
            if (color.equals("gray")) {
                for (String leftGame : new String[]{" left", " left the game"}) {
                    if (text.endsWith(leftGame)) {
                        return new NormalLeftTheGame(text.substring(0, text.length() - leftGame.length()));
                    }
                }
                throw new IllegalStateException("bad gray");
            }
            if (color.equals("gold")) {
                return new GoldCommandResponse(text);
            }
            if (color.equals("dark_red")) {
                return new DarkRedCommandResponse(text);
            }
            if (color.equals("dark_aqua")) {
                return new DarkAquaCommandResponse(text);
            }
            throw new IllegalStateException("unrecognized color");
        }
        if (!elems.get(0).toString().equals("{\"text\":\"\"}")) {
            throw new IllegalStateException();
        }

        if (elems.get(1).toString().equals("{\"color\":\"gold\",\"text\":\"[ Ignored players \"}")) {
            // list of ignored players
            return new ListOfIgnoredPlayers();
        }
        if (elems.get(1).toString().equals("{\"color\":\"gray\",\"text\":\"\"}") && elems.get(3).toString().equals("{\"color\":\"gray\",\"text\":\" joined\"}")) {
            return new NormalJoined(validateAuthor(elems.get(2).getAsJsonObject(), "gray"));
        }
        if (elems.get(1).toString().equals("{\"color\":\"white\",\"text\":\"<\"}")) {
            // chat message
            if (!elems.get(3).toString().equals("{\"color\":\"white\",\"text\":\"> \"}")) {
                throw new IllegalStateException();
            }
            JsonObject author = elems.get(2).getAsJsonObject();
            String realAuthor = validateAuthor(author, "white");
            return new PlayerChat(realAuthor, extract(elems, 4, Arrays.asList("white", "green")));
        }
        if (elems.get(1).toString().equals("{\"color\":\"light_purple\",\"text\":\"\"}")) {
            // incoming whisper
            if (elems.size() < 4) {
                throw new IllegalStateException("incoming");
            }
            String author = validateAuthor(elems.get(2).getAsJsonObject(), "light_purple");
            String msg = extract(elems, 3, Collections.singletonList("light_purple"));
            return new Whisper(author, trim(msg, " whispers: ", ""), false);
        }
        if (elems.get(1).toString().equals("{\"color\":\"light_purple\",\"text\":\"to \"}")) {
            // outgoing whisper
            if (elems.size() < 4) {
                throw new IllegalStateException("outgoing");
            }
            String author = validateAuthor(elems.get(2).getAsJsonObject(), "light_purple");
            String msg = extract(elems, 3, Collections.singletonList("light_purple"));
            return new Whisper(author, trim(msg, ": ", ""), true);
        }

        int start;
        if (elems.get(1).toString().equals("{\"color\":\"dark_aqua\",\"text\":\"\"}")) {
            start = 2;
        } else if (elems.get(1).toString().equals("{\"color\":\"dark_red\",\"text\":\"The Zone claims \"}")) {
            start = 1;
        } else {
            throw new IllegalStateException("Don't know how to parse " + json + " " + chatType);
        }
        //System.out.println(json);
        StringBuilder template = new StringBuilder();
        int playerIdx = 0;
        String[] players = new String[2];
        for (int i = start; i < elems.size(); i++) {
            JsonObject obj = elems.get(i).getAsJsonObject();
            String color = obj.get("color").getAsString();
            if (!color.equals("dark_aqua") && !color.equals("dark_red") && !color.equals("gold")) {
                throw new IllegalStateException("wrong color");
            }
            if (obj.size() == 4 && color.equals("dark_aqua")) {
                // player
                String player = validateAuthor(obj, "dark_aqua");
                players[playerIdx] = player;
                playerIdx++;
                template.append('#');
                template.append(playerIdx);
                continue;
            }
            if (obj.size() == 3 && color.equals("gold")) {
                // item (e.g. killed with Diamond Axe)
                template.append('?');
                continue;
            }
            if (obj.size() == 2) {
                String text = obj.get("text").getAsString();
                if (text.isEmpty()) {
                    continue;
                }
                if (color.equals("gold")) {
                    if (text.equals(" ") && !template.toString().endsWith("named ")) { // handle the case of a wither named " "
                        template.append(' '); // this is needed for "killed by a zombie wielding ?"
                    } else {
                        template.append('?');
                    }
                    continue;
                }
                template.append(text);
                continue;
            }
            throw new IllegalStateException("uwu");
        }
        return new Death(template.toString(), players[0], players[1]);
    }

    private static String extract(JsonArray elems, int start, List<String> colors) {
        StringBuilder msg = new StringBuilder();
        for (int i = start; i < elems.size(); i++) {
            JsonObject comp = elems.get(i).getAsJsonObject();
            String color = comp.get("color").getAsString();
            String text = comp.get("text").getAsString();
            if (comp.size() == 2) {
                if (colors.contains(color)) {
                    msg.append(text);
                    continue;
                }
            }
            if (comp.size() == 3 && comp.has("clickEvent")) {
                JsonObject clickEvent = comp.get("clickEvent").getAsJsonObject();
                if (clickEvent.size() != 2 || !clickEvent.get("action").getAsString().equals("open_url")) {
                    throw new IllegalStateException();
                }
                String url = clickEvent.get("value").getAsString();
                if (url.equals(text) || url.equals("http://" + text)) {
                    msg.append(text);
                    continue;
                } else {
                    throw new IllegalStateException("how did it get from " + text + " to " + url);
                }
            }
            throw new IllegalStateException();
        }
        return msg.toString();
    }

    private static String validateAuthor(JsonObject author, String color) {
        if (author.size() != 4 || !author.get("color").getAsString().equals(color)) {
            throw new IllegalStateException();
        }
        String realAuthor = author.get("text").getAsString();
        JsonObject clickEvent = author.get("clickEvent").getAsJsonObject();
        if (clickEvent.size() != 2 || !clickEvent.get("action").getAsString().equals("suggest_command")) {
            throw new IllegalStateException();
        }
        String clickAuthor = trim(clickEvent.get("value").getAsString(), "/w ", " ");
        String hoverEventAuthor = trim(author.get("hoverEvent").toString(), "{\"action\":\"show_text\",\"value\":{\"extra\":[{\"color\":\"gold\",\"text\":\"Message \"},{\"color\":\"dark_aqua\",\"text\":\"\"},{\"color\":\"dark_aqua\",\"text\":\"", "\"},{\"color\":\"dark_aqua\",\"text\":\"\"}],\"text\":\"\"}}");
        if (!clickAuthor.equals(hoverEventAuthor) || !realAuthor.equals(hoverEventAuthor)) {
            throw new IllegalStateException();
        }
        return realAuthor;
    }


    private static class JsonCache {

        Object2IntOpenHashMap<String> raw = new Object2IntOpenHashMap<>();
        IntOpenHashSet indexWithChatType = new IntOpenHashSet();

        {
            raw.defaultReturnValue(-1);
        }

        private static int encode(int raw, int chatType) {
            return raw * CHAT_TYPES + chatType;
        }

        private int enroll(String line, short chatType) {
            if (chatType < 0 || chatType > 2) {
                throw new IllegalStateException();
            }
            int val = raw.getInt(line);
            if (val == -1) {
                val = raw.size();
                raw.put(line, val);
                indexWithChatType.add(encode(val, chatType));
            }
            return val;
        }

        private void doParseInto(List<RawChatRow> rows) {
            //JsonObject[] json = new JsonObject[raw.size()];
            ParsedChatRow[] parsed = new ParsedChatRow[raw.size() * CHAT_TYPES];
            new ArrayList<>(raw.object2IntEntrySet()).parallelStream().forEach(entry -> {
                int key = entry.getIntValue();
                JsonObject elem = JsonParser.parseString(entry.getKey()).getAsJsonObject();
                //json[key] = elem;
                for (int i = 0; i < CHAT_TYPES; i++) {
                    int pos = encode(key, i);
                    if (indexWithChatType.contains(pos)) {
                        try {
                            parsed[pos] = parseChatRow(elem, i);
                        } catch (Throwable th) {
                            //System.out.println(elem);
                            throw new RuntimeException(elem + "", th);
                        }
                    }
                }
            });
            for (RawChatRow row : rows) {
                //row.asJson = json[row.chatKey];
                row.asParsed = parsed[encode(row.chatKey, row.chatType)];
            }
        }
    }

    public synchronized boolean run() {
        try (Connection connection = Database.getConnection()) {
            connection.setAutoCommit(false);
            long startAt;
            try (PreparedStatement stmt = connection.prepareStatement("SELECT max_created_at_processed FROM chat_progress");
                 ResultSet rs = stmt.executeQuery()) {
                rs.next();
                startAt = rs.getLong("max_created_at_processed");
            }
            System.out.println("Chat processor at " + startAt);
            long age = System.currentTimeMillis() - startAt;
            if (age < TimeUnit.MINUTES.toMillis(5)) {
                System.out.println("Chat is up to date within 5 minutes, exiting");
                return false;
            }
            //startAt = 1620000000000L;
            //startAt = 1625339308300L;
            List<RawChatRow> rawRows = new ArrayList<>();
            JsonCache cache = new JsonCache();
            try (PreparedStatement stmt = connection.prepareStatement("SELECT data, chat_type, reported_by, created_at, server_id FROM chat WHERE created_at > ? ORDER BY created_at LIMIT ?")) {
                stmt.setLong(1, startAt);
                stmt.setInt(2, MAX_ROWS);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        rawRows.add(new RawChatRow(rs, cache));
                    }
                }
            }
            if (rawRows.isEmpty()) {
                System.out.println("No chat!");
                return false;
            }
            cache.doParseInto(rawRows);
            long endFence = rawRows.get(rawRows.size() - 1).createdAt - DEDUPLICATE_WITHIN;
            int firstRowInCoda = binarySearch(rawRows, endFence); // first row with createdAt > endFence. aka, the first row that will be seen the next time this function runs!

            ArrayDeque<Deduplication> activeDedups = new ArrayDeque<>();
            List<RawChatRow> output = new ArrayList<>();
            outer:
            for (int i = 0; i < rawRows.size(); i++) {
                boolean coda = i >= firstRowInCoda;
                RawChatRow row = rawRows.get(i);
                while (!activeDedups.isEmpty() && activeDedups.peekFirst().start <= row.createdAt - DEDUPLICATE_WITHIN) {
                    output.add(activeDedups.removeFirst().contents.get(0));
                }
                if (row.asParsed.deduplicateBetweenPlayers()) {
                    mid:
                    for (Deduplication dedup : activeDedups) {
                        if (row.equalityKey == dedup.equalityKey) {
                            for (RawChatRow prev : dedup.contents) {
                                if (prev.reportedBy == row.reportedBy) {
                                    continue mid;
                                }
                            }
                            if (coda) {
                                activeDedups.remove(dedup);
                            } else {
                                dedup.contents.add(row);
                            }
                            continue outer;
                        }
                    }
                    if (!coda) {
                        activeDedups.add(new Deduplication(row));
                    }
                } else {
                    if (!coda) {
                        output.add(row);
                    }
                }
            }
            if (!activeDedups.isEmpty()) {
                throw new IllegalStateException();
            }

            for (RawChatRow row : output) {
                row.asParsed.save(connection, row);
            }
            try (PreparedStatement stmt = connection.prepareStatement("UPDATE chat_progress SET max_created_at_processed = ?")) {
                stmt.setLong(1, endFence);
                stmt.execute();
            }
            System.out.println("Chat processor committing " + output.size() + " rows from input of " + rawRows.size() + " rows");
            connection.commit();
            Database.incrementCommitCounter("chat_processor");
            return !output.isEmpty();
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    private static class Deduplication {

        private final List<RawChatRow> contents;
        private final long start;
        private final int equalityKey;

        private Deduplication(RawChatRow row) {
            this.contents = new ArrayList<>();
            contents.add(row);
            this.start = row.createdAt;
            this.equalityKey = row.equalityKey;
        }
    }

    private static int binarySearch(List<RawChatRow> rows, long firstRowNewerThan) {
        int lo = 0;
        int hi = rows.size() - 1;
        if (hi < lo) {
            throw new IllegalStateException();
        }
        while (lo < hi) {
            int mid = (hi + lo) / 2;
            if (firstRowNewerThan < rows.get(mid).createdAt) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if (firstRowNewerThan >= rows.get(hi).createdAt) {
            throw new IllegalStateException();
        }
        if (hi > 0) {
            long prev = rows.get(hi - 1).createdAt;
            if (prev > firstRowNewerThan) {
                throw new IllegalStateException();
            }
        }
        return hi;
    }
}
