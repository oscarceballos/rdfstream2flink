package rdfstream2flink.mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LoadQueryFile {

    private String queryFile;

    public LoadQueryFile(String queryFile){
        this.queryFile = queryFile;
    }

    public String loadSQFile() {
        String line="", query="";
        try{
            File inputFile = new File(queryFile);
            FileReader in = new FileReader(inputFile);
            BufferedReader inputStream = new BufferedReader(in);
            while((line = inputStream.readLine()) != null) {
                query += line + "\n" ;
            }
            in.close();
        }catch(IOException e) {
            System.err.format("IOException: %s%n", e);
        }
        return query;
    }

    public String maskUris(String host, int initialPort) throws Exception {
        String query = this.loadSQFile();
        int port = initialPort;

        String newQuery = "";
        Pattern pattern = Pattern.compile("([ ]*<[ ]*)([-./:a-zA-Z0-9#]*)([ ]*>[ ]*\\[[ a-zA-Z0-9]*\\])");
        Matcher matcher;
        String[] streamSegments = query.trim().replace("STREAM", "stream").split("stream");
        if (streamSegments.length == 1)
            throw new Exception("Error parsing query, no stream statements found for: " + query);
        else {
            for (String s : streamSegments) {
                matcher = pattern.matcher(s);
                if(matcher.find() && matcher.groupCount()>=3) {
                    newQuery += matcher.replaceFirst("STREAM" + matcher.group(1) + host + ":" + port + matcher.group(3));
                    port++;
                } else newQuery += s;
            }
        }

        if(newQuery.equals("")) newQuery = query;
        return newQuery;
    }
}
