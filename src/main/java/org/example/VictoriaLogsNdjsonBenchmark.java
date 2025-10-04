package org.example;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdOutputStream;

/**
 * NDJSON concurrent ingestion benchmark for VictoriaLogs.
 * Config via args: key=value, e.g.
 * threads=16 duration=30 lineSize=1024 payloadBytes=33554432 gzip=true endpoint=http://localhost:9428/insert/jsonline
 */
public class VictoriaLogsNdjsonBenchmark {
    public static void main(String[] args) throws Exception {
        Map<String, String> cfg = parseArgs(args);
        String endpoint = cfg.getOrDefault("endpoint", "http://localhost:9428/insert/jsonline");
        String query = cfg.getOrDefault("query", "?_stream_fields=stream&_time_field=date&_msg_field=log.message");
        int threads = Integer.parseInt(cfg.getOrDefault("threads", "16"));
        int durationSec = Integer.parseInt(cfg.getOrDefault("duration", cfg.getOrDefault("durationSec", "3600")));
        int lineSize = Integer.parseInt(cfg.getOrDefault("lineSize", "1024"));
        int payloadBytes = Integer.parseInt(cfg.getOrDefault("payloadBytes", String.valueOf(512 * 1024 * 1024))); // 32MB
        String codec = cfg.getOrDefault("codec", "gzip"); // gzip | zstd | none
        boolean gzip = "gzip".equalsIgnoreCase(codec);
        boolean zstd = "zstd".equalsIgnoreCase(codec);
        int zstdLevel = Integer.parseInt(cfg.getOrDefault("zstdLevel", "3"));
        String stream = cfg.getOrDefault("stream", "benchmark");
        boolean streaming = Boolean.parseBoolean(cfg.getOrDefault("streaming", "true"));
        String httpVersion = cfg.getOrDefault("httpVersion", "1.1"); // 1.1 or 2

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .version("2".equals(httpVersion) ? java.net.http.HttpClient.Version.HTTP_2 : java.net.http.HttpClient.Version.HTTP_1_1)
                .build();

        AtomicLong totalLines = new AtomicLong();
        AtomicLong totalBytes = new AtomicLong();
        AtomicLong totalRequests = new AtomicLong();
        AtomicLong totalErrors = new AtomicLong();
        AtomicLong totalUncompressedBytes = new AtomicLong();
        AtomicReference<ConcurrentLinkedQueue<Long>> latenciesMs = new AtomicReference<>(new ConcurrentLinkedQueue<>());

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        long benchStartNs = System.nanoTime();
        long endAt = benchStartNs + TimeUnit.SECONDS.toNanos(durationSec);

        Runnable worker = () -> {
            try {
                while (System.nanoTime() < endAt) {
                HttpRequest req;
                int rawBytes;
                int lines;
                HttpRequest.Builder rb = HttpRequest.newBuilder()
                        .uri(URI.create(endpoint + query))
                        .timeout(Duration.ofSeconds(30))
                        .header("Content-Type", "application/stream+json");
                if (streaming) {
                    PayloadMeta meta = computePayloadMeta(lineSize, payloadBytes, stream);
                    rawBytes = meta.rawBytes;
                    lines = meta.lines;
                    AtomicLong reqCompressedBytes = new AtomicLong();
                    HttpRequest.BodyPublisher bp = streamingBodyPublisher(lineSize, lines, stream, gzip, zstd, zstdLevel, reqCompressedBytes);
                    if (gzip) rb.header("Content-Encoding", "gzip");
                    else if (zstd) rb.header("Content-Encoding", "zstd");
                    req = rb.POST(bp).build();
                    long start = System.nanoTime();
                    try {
                        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                        if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                            totalLines.addAndGet(lines);
                            totalBytes.addAndGet(reqCompressedBytes.get());
                            totalUncompressedBytes.addAndGet(rawBytes);
                            totalRequests.incrementAndGet();
                            latenciesMs.get().add((System.nanoTime() - start) / 1_000_000); // ms
                        } else {
                            totalErrors.incrementAndGet();
                        }
                    } catch (Exception e) {
                        totalErrors.incrementAndGet();
                    }
                    continue; // next loop iteration
                } else {
                    Payload p = buildNdjsonPayload(lineSize, payloadBytes, stream);
                    byte[] body = p.data;
                    rawBytes = body.length; // size before compression
                    lines = p.lines;
                    if (gzip) body = gzip(body);
                    else if (zstd) body = zstd(body, zstdLevel);
                    if (gzip) rb.header("Content-Encoding", "gzip");
                    else if (zstd) rb.header("Content-Encoding", "zstd");
                    req = rb.POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
                    long start = System.nanoTime();
                    try {
                        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                        if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                            totalLines.addAndGet(lines);
                            totalBytes.addAndGet(body.length);
                            totalUncompressedBytes.addAndGet(rawBytes);
                            totalRequests.incrementAndGet();
                            latenciesMs.get().add((System.nanoTime() - start) / 1_000_000); // ms
                        } else {
                            totalErrors.incrementAndGet();
                        }
                    } catch (Exception e) {
                        // Ignore transient errors in benchmark loop
                        totalErrors.incrementAndGet();
                    } finally {
                        // no-op
                    }
                }
            } catch (Throwable t) {
                // Swallow interrupts on shutdown and unexpected errors to avoid noisy stacktraces
                totalErrors.incrementAndGet();
            }
        };

        for (int i = 0; i < threads; i++) pool.submit(worker);

        long lastLines = 0, lastBytes = 0, lastErrors = 0, lastRequests = 0, lastUncompressedBytes = 0;
        long lastTs = System.nanoTime();
        while (System.nanoTime() < endAt) {
            Thread.sleep(1000);
            long now = System.nanoTime();
            long lines = totalLines.get();
            long bytes = totalBytes.get();
            long rawBytes = totalUncompressedBytes.get();
            long requests = totalRequests.get();
            long errors = totalErrors.get();
            double seconds = (now - lastTs) / 1_000_000_000.0;
            long dLines = lines - lastLines;
            long dBytes = bytes - lastBytes;
            long dRawBytes = rawBytes - lastUncompressedBytes;
            long dReq = requests - lastRequests;
            long dErr = errors - lastErrors;
            double lps = dLines / seconds;
            double bps = dBytes / seconds;
            double rbps = dRawBytes / seconds;
            // swap latency buffer and compute percentiles for the last second
            ConcurrentLinkedQueue<Long> latQ = latenciesMs.getAndSet(new ConcurrentLinkedQueue<>());
            int n = latQ.size();
            long[] arr = new long[n];
            int i = 0;
            long sum = 0;
            long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
            while (!latQ.isEmpty()) {
                Long v = latQ.poll();
                if (v == null) break;
                arr[i++] = v;
                sum += v;
                if (v < min) min = v;
                if (v > max) max = v;
            }
            if (i > 0) {
                java.util.Arrays.sort(arr, 0, i);
                long p50 = arr[(int)Math.floor(i * 0.50) - 1 < 0 ? 0 : (int)Math.floor(i * 0.50) - 1];
                long p95 = arr[(int)Math.floor(i * 0.95) - 1 < 0 ? 0 : (int)Math.floor(i * 0.95) - 1];
                long p99 = arr[(int)Math.floor(i * 0.99) - 1 < 0 ? 0 : (int)Math.floor(i * 0.99) - 1];
                double avg = sum * 1.0 / i;
                System.out.printf(
                        "seconds=%d, threads=%d, lps=%.0f lines/s, bps=%.2f GB/s, raw_bps=%.2f GB/s, req=%d (+%d), err(+%d), lat(ms): avg=%.2f p50=%d p95=%d p99=%d min=%d max=%d\n",
                        (int)seconds, threads, lps, bps / (1024.0 * 1024.0 * 1024.0), rbps / (1024.0 * 1024.0 * 1024.0), requests, dReq, dErr,
                        avg, p50, p95, p99, min, max);
            } else {
                System.out.printf(
                        "seconds=%d, threads=%d, lps=%.0f lines/s, bps=%.2f GB/s, raw_bps=%.2f GB/s, req=%d (+%d), err(+%d)\n",
                        (int)seconds, threads, lps, bps / (1024.0 * 1024.0 * 1024.0), rbps / (1024.0 * 1024.0 * 1024.0), requests, dReq, dErr);
            }
            lastLines = lines; lastBytes = bytes; lastUncompressedBytes = rawBytes; lastTs = now;
            lastErrors = errors; lastRequests = requests;
        }

        // Use graceful shutdown to avoid interrupting in-flight requests and printing stack traces
        pool.shutdown();
        try { pool.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        double elapsedSec = (System.nanoTime() - benchStartNs) / 1_000_000_000.0;
        System.out.printf("Total: elapsed=%.2f s, lines=%d, bytes=%.2f GB, bytes_raw=%.2f GB, requests=%d, errors=%d\n",
                elapsedSec, totalLines.get(), totalBytes.get() / (1024.0 * 1024.0 * 1024.0), totalUncompressedBytes.get() / (1024.0 * 1024.0 * 1024.0), totalRequests.get(), totalErrors.get());
    }

    private static Map<String, String> parseArgs(String[] args) {
        return Arrays.stream(args == null ? new String[0] : args)
                .map(s -> s.split("=", 2))
                .filter(kv -> kv.length == 2)
                .collect(java.util.stream.Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }

    private static class Payload {
        final byte[] data;
        final int lines;
        Payload(byte[] d, int l) { this.data = d; this.lines = l; }
    }

    private static class PayloadMeta {
        final int lines;
        final int rawBytes;
        PayloadMeta(int lines, int rawBytes) { this.lines = lines; this.rawBytes = rawBytes; }
    }

    private static Payload buildNdjsonPayload(int lineSize, int targetBytes, String stream) {
        final byte[] prefix = ("{\"log\":{\"level\":\"info\",\"message\":\"").getBytes(StandardCharsets.UTF_8);
        final byte[] mid = ("\"},\"date\":\"0\",\"stream\":\"").getBytes(StandardCharsets.UTF_8);
        final byte[] suffix = ("\"}\n").getBytes(StandardCharsets.UTF_8);
        byte[] msg = new byte[lineSize];
        // Fill with random alphanumeric ASCII to reduce gzip compressibility
        // and stay JSON-safe (no quotes/backslashes/newlines).
        java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
        for (int i = 0; i < msg.length; i++) {
            int r = rnd.nextInt(36); // 26 letters + 10 digits
            msg[i] = (byte) (r < 26 ? ('a' + r) : ('0' + (r - 26)));
        }
        byte[] streamBytes = stream.getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream out = new ByteArrayOutputStream(targetBytes + 1024);
        int lines = 0;
        int estLine = prefix.length + lineSize + mid.length + streamBytes.length + suffix.length;
        while (out.size() + estLine <= targetBytes) {
            out.write(prefix, 0, prefix.length);
            out.write(msg, 0, msg.length);
            out.write(mid, 0, mid.length);
            out.write(streamBytes, 0, streamBytes.length);
            out.write(suffix, 0, suffix.length);
            lines++;
        }
        return new Payload(out.toByteArray(), lines);
    }

    private static PayloadMeta computePayloadMeta(int lineSize, int targetBytes, String stream) {
        final byte[] prefix = ("{\"log\":{\"level\":\"info\",\"message\":\"").getBytes(StandardCharsets.UTF_8);
        final byte[] mid = ("\"},\"date\":\"0\",\"stream\":\"").getBytes(StandardCharsets.UTF_8);
        final byte[] suffix = ("\"}\n").getBytes(StandardCharsets.UTF_8);
        int estLine = prefix.length + lineSize + mid.length + stream.getBytes(StandardCharsets.UTF_8).length + suffix.length;
        int lines = targetBytes / estLine;
        int rawBytes = lines * estLine;
        return new PayloadMeta(lines, rawBytes);
    }

    private static HttpRequest.BodyPublisher streamingBodyPublisher(int lineSize, int lines, String stream,
                                                                   boolean gzip, boolean zstd, int zstdLevel,
                                                                   AtomicLong compressedBytesOut) {
        return HttpRequest.BodyPublishers.ofInputStream(() -> {
            try {
                final int pipeBuffer = 64 * 1024;
                PipedInputStream pis = new PipedInputStream(pipeBuffer);
                PipedOutputStream pos = new PipedOutputStream(pis);

                Thread t = new Thread(() -> {
                    try {
                        OutputStream countedUnderlying = new CountingOutputStream(pos, compressedBytesOut);
                        OutputStream os = countedUnderlying;
                        if (gzip) {
                            os = new java.util.zip.GZIPOutputStream(os, true);
                        } else if (zstd) {
                            os = new ZstdOutputStream(os, zstdLevel);
                        }
                        writeNdjson(os, lineSize, lines, stream);
                        os.close(); // also closes underlying streams
                    } catch (Exception e) {
                        try { pos.close(); } catch (IOException ignore) {}
                    }
                }, "ndjson-writer");
                t.setDaemon(true);
                t.start();
                return pis;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void writeNdjson(OutputStream out, int lineSize, int lines, String stream) throws IOException {
        final byte[] prefix = ("{\"log\":{\"level\":\"info\",\"message\":\"").getBytes(StandardCharsets.UTF_8);
        final byte[] mid = ("\"},\"date\":\"0\",\"stream\":\"").getBytes(StandardCharsets.UTF_8);
        final byte[] suffix = ("\"}\n").getBytes(StandardCharsets.UTF_8);
        byte[] streamBytes = stream.getBytes(StandardCharsets.UTF_8);
        java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
        byte[] msg = new byte[lineSize];
        for (int i = 0; i < lines; i++) {
            for (int j = 0; j < msg.length; j++) {
                int r = rnd.nextInt(36);
                msg[j] = (byte) (r < 26 ? ('a' + r) : ('0' + (r - 26)));
            }
            out.write(prefix);
            out.write(msg);
            out.write(mid);
            out.write(streamBytes);
            out.write(suffix);
        }
        out.flush();
    }

    private static class CountingOutputStream extends OutputStream {
        private final OutputStream delegate;
        private final AtomicLong counter;
        CountingOutputStream(OutputStream delegate, AtomicLong counter) { this.delegate = delegate; this.counter = counter; }
        @Override public void write(int b) throws IOException { delegate.write(b); counter.incrementAndGet(); }
        @Override public void write(byte[] b, int off, int len) throws IOException { delegate.write(b, off, len); counter.addAndGet(len); }
        @Override public void write(byte[] b) throws IOException { delegate.write(b); counter.addAndGet(b.length); }
        @Override public void flush() throws IOException { delegate.flush(); }
        @Override public void close() throws IOException { delegate.close(); }
    }

    private static byte[] gzip(byte[] src) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(src.length / 2);
            java.util.zip.GZIPOutputStream gz = new java.util.zip.GZIPOutputStream(out, true);
            gz.write(src);
            gz.finish();
            gz.close();
            return out.toByteArray();
        } catch (Exception e) {
            return src; // fallback
        }
    }

    private static byte[] zstd(byte[] src, int level) {
        try {
            return Zstd.compress(src, level);
        } catch (Throwable t) {
            return src; // fallback
        }
    }
}