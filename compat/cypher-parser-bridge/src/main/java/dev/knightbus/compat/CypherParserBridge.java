package dev.knightbus.compat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.neo4j.cypherdsl.parser.CypherParser;

public final class CypherParserBridge {
    private static final String ARTIFACT =
            "org.neo4j:neo4j-cypher-dsl-parser:2025.1.0";

    private CypherParserBridge() {}

    public static void main(String[] args) throws Exception {
        var reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String encodedQuery;
        while ((encodedQuery = reader.readLine()) != null) {
            if (encodedQuery.isBlank()) {
                continue;
            }
            var query = new String(
                    Base64.getDecoder().decode(encodedQuery), StandardCharsets.UTF_8);
            try {
                CypherParser.parseStatement(query);
                System.out.println("{\"artifact\":\"" + ARTIFACT
                        + "\",\"outcome\":\"accepted\"}");
            } catch (RuntimeException error) {
                System.out.println("{\"artifact\":\"" + ARTIFACT
                        + "\",\"outcome\":\"syntax\"}");
            }
        }
    }
}
