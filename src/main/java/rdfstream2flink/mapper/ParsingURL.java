package rdfstream2flink.mapper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParsingURL {
    private static String server;
    private static Integer port;
    private static String name;

    private static String PATTERN_STR = "(http[s]?://[a-zA-Z0-9]*):([0-9]*)/(([a-zA-Z0-9/]*))#([a-zA-Z0-9]*)";

    public static String getServer() {
        return server;
    }

    public static void setServer(String server) {
        ParsingURL.server = server;
    }

    public static Integer getPort() {
        return port;
    }

    public static void setPort(Integer port) {
        ParsingURL.port = port;
    }

    public static String getName() {
        return name;
    }

    public static void setName(String name) {
        ParsingURL.name = name;
    }

    public static void parsing(String url) {
        Pattern pattern = Pattern.compile(PATTERN_STR);
        Matcher matcher = pattern.matcher(url);
        if(matcher.find()) {
            server = matcher.group(1);
            port = Integer.parseInt(matcher.group(2));
            name = matcher.group(4);
        }
    }
}
