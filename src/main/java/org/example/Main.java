package org.example;

import io.greptime.BulkStreamWriter;
import io.greptime.BulkWrite;
import io.greptime.GreptimeDB;
import io.greptime.models.AuthInfo;
import io.greptime.models.DataType;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.options.GreptimeOptions;
import io.greptime.rpc.Compression;
import io.greptime.rpc.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

//TIP 要<b>运行</b>代码，请按 <shortcut actionId="Run"/> 或
// 点击装订区域中的 <icon src="AllIcons.Actions.Execute"/> 图标。
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        //TIP 当文本光标位于高亮显示的文本处时按 <shortcut actionId="ShowIntentionActions"/>
        // 查看 IntelliJ IDEA 建议如何修正。
        // GreptimeDB 在默认目录 "greptime" 中有一个名为 "public" 的默认数据库，
        // 我们可以将其用作测试数据库
        String database = "public";
        // 默认情况下，GreptimeDB 使用 gRPC 协议在端口 4001 上监听。
        // 我们可以提供多个指向同一 GreptimeDB 集群的端点。
        // 客户端将基于负载均衡策略调用这些端点。
        // 客户端执行定期健康检查并自动将请求路由到健康节点，
        // 为你的应用程序提供容错能力和改进的可靠性。
        String[] endpoints = {"127.0.0.1:4001"};
        // 设置认证信息。
        AuthInfo authInfo = AuthInfo.noAuthorization();
        GreptimeOptions opts = GreptimeOptions.newBuilder(endpoints, database)
                // 如果数据库不需要认证，我们可以使用 `AuthInfo.noAuthorization()` 作为参数。
                .authInfo(authInfo)
                // 如果你的服务器由 TLS 保护，请启用安全连接
                //.tlsOptions(new TlsOptions())
                // 好的开始 ^_^
                .build();

        // 初始化客户端
        // 注意：客户端实例是线程安全的，应作为全局单例重用
        // 以获得更好的性能和资源利用率。
        GreptimeDB greptimeDB = GreptimeDB.create(opts);
        // 使用表结构创建 BulkStreamWriter
        BulkWrite.Config cfg = BulkWrite.Config.newBuilder()
                .allocatorInitReservation(2 * 1024 * 1024 * 1024L) // 初始保留 32MB，降低内存启动压力
                .allocatorMaxAllocation(12L * 1024 * 1024 * 1024) // 最大分配 2GB，避免超大批次导致 OOM
                .timeoutMsPerMessage(60 * 1000) // 每个请求 60 秒超时
                .maxRequestsInFlight(32) // 降低并发请求上限，缓解内存峰值
                .build();
        // 关闭压缩以降低内存峰值（如需网络带宽优先，可改回 Zstd）
        Context ctx = Context.newDefault().withCompression(Compression.Zstd);

        TableSchema schema = TableSchema.newBuilder("event_logs_20251004")
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("ip", DataType.String)
                .addField("port", DataType.Int32)
                .addField("protocal", DataType.String)
                .addField("event_body", DataType.Binary)
                .build();

        List<CompletableFuture<Integer>> futures = new ArrayList<>();

        long start = System.currentTimeMillis();
        try (BulkStreamWriter writer = greptimeDB.bulkStreamWriter(schema, cfg, ctx)) {
            // 回归安全模式：单线程顺序构建并写出每个批次
            int batchCount = 32;
            int rowsPerBatch = 256 * 1024; // 控制单批次大小，降低构建与内存峰值
            int bufferPerTableBytes = 1024 * 1024 ; // 每批次列缓冲约 2MB

            for (int i = 0; i < batchCount; i++) {
                Table.TableBufferRoot table = writer.tableBufferRoot(bufferPerTableBytes);
                System.out.println("classname:" + table.getClass().getName());

                long addRowStartTime = System.currentTimeMillis();
                for (int row = 0; row < rowsPerBatch; row++) {
                    byte[] eventBody = to7KBBytes(i, row);
                    table.addRow(
                            System.currentTimeMillis(), // ts
                            "192.168.1." + (row % 256), // ip
                            8080, // port
                            "http", // protocal
                            eventBody // event_body
                    );
                    if (row % 200000 == 0) {
                        System.out.println("Batch " + i + " addRow elapsed: " + (System.currentTimeMillis() - addRowStartTime) + " ms");
                    }
                }

                long completeStart = System.currentTimeMillis();
                System.out.println("Batch " + i + " table complete cost: " + (System.currentTimeMillis() - completeStart) + " ms, bytesUsed: " + table.bytesUsed());
                table.complete();
                System.out.println("Batch " + i + " table complete cost: " + (System.currentTimeMillis() - completeStart) + " ms, bytesUsed: " + table.bytesUsed());

                long writeStart = System.currentTimeMillis();
                CompletableFuture<Integer> future = writer.writeNext();
                futures.add(future);
                System.out.println("Batch " + i + " writeNext cost: " + (System.currentTimeMillis() - writeStart) + " ms");
            }

            // 等待所有批次写出完成并结束流
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            writer.completed();
        }
        long end = System.currentTimeMillis();
        System.out.println("Total time: " + (end - start) + " ms");
    }

    private static byte[] to7KBBytes(int batchIndex, int row) {
        int target = 1024 * 4; // 7KB
        byte[] out = new byte[target];
        String header = "event_" + batchIndex + '_' + row + '_' + System.currentTimeMillis();
        byte[] headerBytes = header.getBytes(StandardCharsets.US_ASCII);
        int copyLen = Math.min(headerBytes.length, target);
        System.arraycopy(headerBytes, 0, out, 0, copyLen);
        for (int i = copyLen; i < target; i++) {
            out[i] = 'a';
        }
        return out;
    }
}