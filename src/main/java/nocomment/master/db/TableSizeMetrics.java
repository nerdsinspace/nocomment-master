package nocomment.master.db;

import io.prometheus.client.Gauge;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TableSizeMetrics {
    private static final Gauge relationSizes = Gauge.build()
            .name("relation_sizes")
            .help("Relation sizes")
            .labelNames("name")
            .register();

    public static void update() {
        try (Connection connection = Database.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT relname AS name, pg_relation_size(C.oid) AS size FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')");
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                relationSizes.labels(rs.getString("name")).set(rs.getLong("size"));
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
