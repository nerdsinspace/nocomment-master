package nocomment.master.db;

import io.prometheus.client.Gauge;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import shaded.org.apache.commons.io.IOUtils;

import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class TableSizeMetrics {
    private static final Gauge relationSizes = Gauge.build()
            .name("relation_sizes")
            .help("Relation sizes")
            .labelNames("name")
            .register();
    private static final Gauge relationCompressedSizes = Gauge.build()
            .name("relation_compressed_sizes")
            .help("Relation compressed sizes")
            .labelNames("name")
            .register();
    private static final Gauge relationRows = Gauge.build()
            .name("relation_rows")
            .help("Relation rows")
            .labelNames("name")
            .register();

    private static final Set<String> tables = new HashSet<>();

    public static synchronized void update() {
        Object2LongOpenHashMap<String> compressedSizes = getDiskUsageSummary();
        try (Connection connection = Database.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT relname AS name, pg_relation_size(C.oid) AS size, pg_relation_filepath(C.oid) AS path, reltuples as rows FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')");
             ResultSet rs = stmt.executeQuery()) {
            Set<String> seenThisTime = new HashSet<>();
            while (rs.next()) {
                String name = rs.getString("name");
                relationSizes.labels(name).set(rs.getLong("size"));
                long sz = compressedSizes.getLong(rs.getString("path"));
                if (sz != 0) {
                    relationCompressedSizes.labels(name).set(sz);
                }
                relationRows.labels(name).set(rs.getLong("rows"));
                seenThisTime.add(name);
            }
            for (String lastTime : tables) {
                if (!seenThisTime.contains(lastTime)) {
                    relationSizes.remove(lastTime);
                    relationCompressedSizes.remove(lastTime);
                    relationRows.remove(lastTime);
                }
            }
            tables.clear();
            tables.addAll(seenThisTime);
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    public static Object2LongOpenHashMap<String> getDiskUsageSummary() {
        Object2LongOpenHashMap<String> map = new Object2LongOpenHashMap<>();
        try {
            IOUtils.readLines(new InputStreamReader(new ProcessBuilder("bash", "-c", "du -s /postgres/base/16385/*").redirectError(ProcessBuilder.Redirect.INHERIT).start().getInputStream())).forEach(line -> map.addTo(line.split("\t")[1].split("\\.")[0].split("/postgres/")[1], Long.parseLong(line.split("\t")[0]) * 1024L));
        } catch (Throwable th) {
            th.printStackTrace();
        }
        return map;
    }
}
