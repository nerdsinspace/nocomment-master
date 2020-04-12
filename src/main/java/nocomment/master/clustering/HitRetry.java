package nocomment.master.clustering;

import nocomment.master.db.Database;
import nocomment.master.util.ChunkPos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public enum HitRetry {
    INSTANCE;

    public ChunkPos clusterTraverse(short serverID, short dimension) {
        try (Connection connection = Database.getConnection()) {
            try (PreparedStatement stmt = connection.prepareStatement("" +
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
                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next();
                    return new ChunkPos(rs.getInt("x"), rs.getInt("z"));
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
