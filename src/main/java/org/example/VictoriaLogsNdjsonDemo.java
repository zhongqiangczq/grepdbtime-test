package org.example;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

/**
 * Simple NDJSON (JSON line stream) ingestion demo for VictoriaLogs.
 * Endpoint docs: /insert/jsonline
 * Run VictoriaLogs locally before executing this demo.
 */
public class VictoriaLogsNdjsonDemo {
    public static void main(String[] args) throws Exception {
        // Change host:port if your VictoriaLogs runs elsewhere
        String url = "http://localhost:9428/insert/jsonline" +
                "?_stream_fields=stream&_time_field=date&_msg_field=log.message";

        // Build NDJSON payload: multiple JSON lines separated by \n
        String ndjson = "{ \"log\": { \"level\": \"info\", \"message\": \"hello ndjson 1\" }, \"date\": \"0\", \"stream\": \"demo\" }\n" +
                "{ \"log\": { \"level\": \"error\", \"message\": \"oh no!\" }, \"date\": \"0\", \"stream\": \"demo\" }\n" +
                "{ \"log\": { \"level\": \"info\", \"message\": \"hello ndjson 3\" }, \"date\": \"0\", \"stream\": \"demo2\" }\n";

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/stream+json")
                .POST(HttpRequest.BodyPublishers.ofString(ndjson, StandardCharsets.UTF_8))
                .build();

        long start = System.currentTimeMillis();
        HttpResponse<String> resp = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        long cost = System.currentTimeMillis() - start;

        System.out.println("Status: " + resp.statusCode());
        System.out.println("Cost: " + cost + " ms");
        System.out.println("Body: " + resp.body());

        // You can verify ingestion via:
        // curl 'http://localhost:9428/select/logsql/query' -d 'query=stream:demo'
    }
}