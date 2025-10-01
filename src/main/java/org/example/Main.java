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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

//TIP 要<b>运行</b>代码，请按 <shortcut actionId="Run"/> 或
// 点击装订区域中的 <icon src="AllIcons.Actions.Execute"/> 图标。
public class Main {
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
                .allocatorInitReservation(64 * 1024 * 1024L) // 自定义内存分配：64MB 初始保留
                .allocatorMaxAllocation(10 * 1024 * 1024 * 1024L) // 自定义内存分配：4GB 最大分配
                .timeoutMsPerMessage(60 * 1000) // 每个请求 60 秒超时
                .maxRequestsInFlight(32) // 并发控制：配置 8 个最大并发请求
                .build();
        // 启用 Zstd 压缩
        Context ctx = Context.newDefault().withCompression(Compression.Zstd);

        TableSchema schema = TableSchema.newBuilder("event_logs")
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("ip", DataType.String)
                .addField("port", DataType.Int32)
                .addField("protocal", DataType.String)
                .addField("event_body", DataType.Binary)
                .build();

        List<Future<Integer>> futures = new ArrayList<>();

        long start = System.currentTimeMillis();
        try (BulkStreamWriter writer = greptimeDB.bulkStreamWriter(schema, cfg, ctx)) {
            // 写入多个批次
            int batchCount = 64;
            int rowsPerBatch = 1 * 51 * 1024;
            for (int batch = 0; batch < batchCount; batch++) {
                // 为此批次获取 TableBufferRoot
                Table.TableBufferRoot table = writer.tableBufferRoot( 1024*51*1024); // 列缓冲区大小



                // 按你的写入API设置列值（示例）：
                for (int row = 0; row < rowsPerBatch; row++) {
                    // 向批次添加行
                    // 例如：构造每条 event 的内容（保证ASCII）
                    String baseBody = "event_" + row + "_" + System.currentTimeMillis();
                    String eventBody = to1KBAscii(baseBody);
                    table.addRow(
                            System.currentTimeMillis(), // ts
                            "192.168.1." + (row % 256), // ip
                            8080, // port
                            "http", // protocal
                            eventBody.getBytes(StandardCharsets.UTF_8) // event_body
                    );
                }

                // 完成表以准备传输
                table.complete();
                System.out.println("Batch " + batch + " cost time: " + (System.currentTimeMillis() - start));
                // 发送批次并获取完成的 future
                CompletableFuture<Integer> future = writer.writeNext();
                futures.add(future);

                System.out.println("Batch " + batch + " wrote " + rowsPerBatch + " rows, cost time: " + (System.currentTimeMillis() - start));
            }

            // 等待批次被处理（可选）
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            // 发出流完成信号
            writer.completed();
        }
        long end = System.currentTimeMillis();
        System.out.println("Total time: " + (end - start) + " ms");
    }

    private static String to10KBAscii(String base) {
        int target = 10240; // 10KB (按UTF-8字节计算)
        // 建议 base 仅使用 ASCII，避免UTF-8多字节字符被截断
        byte[] baseBytes = base.getBytes(StandardCharsets.UTF_8);
        if (baseBytes.length >= target) {
                // 若已超过10KB，按字节安全截断（ASCII不影响字符完整性）
                return new String(baseBytes, 0, target, StandardCharsets.UTF_8);
        }
        StringBuilder sb = new StringBuilder(base);
        int remaining = target - baseBytes.length;
        for (int i = 0; i < remaining; i++) {
                sb.append((char)('a' + (i % 26))); // 用ASCII字符填充
        }
        return sb.toString();
        }
}