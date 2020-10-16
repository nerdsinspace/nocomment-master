package nocomment.master.clustering;

import nocomment.master.db.Database;
import nocomment.master.util.ChunkPos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public final class HitRetry {

    private static final Random RANDOM = new Random();

    public static Optional<ChunkPos> clusterTraverse(short serverID, short dimension) {
        try (Connection connection = Database.getConnection(); PreparedStatement stmt = connection.prepareStatement("" +
                "            WITH RECURSIVE initial AS (                                      " +
                "                SELECT                                                       " +
                "                    id,                                                      " +
                "                    disjoint_rank                                            " +
                "                FROM                                                         " +
                "                    dbscan                                                   " +
                "                WHERE                                                        " +
                "                    cluster_parent IS NULL                                   " +
                "                    AND disjoint_rank > 0                                    " +
                "                    AND server_id = ?                                        " +
                "                    AND dimension = ?                                        " +
                "                    AND root_updated_at > ?                                  " +
                "                ORDER BY RANDOM()                                            " +
                "                LIMIT 1                                                      " +
                "            ),                                                               " +
                "            clusters AS (                                                    " +
                "                SELECT                                                       " +
                "                    id,                                                      " +
                "                    disjoint_rank                                            " +
                "                FROM                                                         " +
                "                    initial                                                  " +
                "                UNION                                                        " +
                "                    SELECT                                                   " +
                "                        dbscan.id,                                           " +
                "                        dbscan.disjoint_rank                                 " +
                "                    FROM                                                     " +
                "                        dbscan                                               " +
                "                    INNER JOIN                                               " +
                "                        clusters                                             " +
                "                            ON dbscan.cluster_parent = clusters.id           " +
                "                    WHERE                                                    " +
                "                        clusters.disjoint_rank > 0                           " +
                "            ), choice AS (                                                   " +
                "                SELECT                                                       " +
                "                    id                                                       " +
                "                FROM                                                         " +
                "                    clusters                                                 " +
                "                ORDER BY RANDOM()                                            " +
                "                LIMIT 1                                                      " +
                "            )                                                                " +
                "            SELECT                                                           " +
                "                x,                                                           " +
                "                z                                                            " +
                "            FROM                                                             " +
                "                dbscan                                                       " +
                "            INNER JOIN choice                                                " +
                "                ON choice.id = dbscan.id                                     ")) {
            stmt.setShort(1, serverID);
            stmt.setShort(2, dimension);
            long mustBeNewerThan;
            if (RANDOM.nextBoolean()) {
                mustBeNewerThan = 0;
            } else if (RANDOM.nextBoolean()) {
                mustBeNewerThan = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30);
            } else {
                mustBeNewerThan = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
            }
            stmt.setLong(3, mustBeNewerThan);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new ChunkPos(rs.getInt("x"), rs.getInt("z")));
                } else {
                    return Optional.empty();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
