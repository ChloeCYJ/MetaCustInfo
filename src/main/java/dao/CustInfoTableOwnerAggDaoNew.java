package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class CustInfoTableOwnerAggDaoNew {

    public OwnerAggregationResult executeOwnerAggregation(Connection conn) throws SQLException {
        int truncated = truncateAggregateTable(conn);
        int inserted = executeUpdate(conn, sqlInsertDistinctTables());
        int prefixUpdated = executeUpdate(conn, sqlUpdateTablePrefix());
        int firstMatched = executeUpdate(conn, sqlMapOwnerByUniqueModuleCode());
        int ddlMapped = executeUpdate(conn, sqlMapUnmatchedToDdl());
        int pdmMapped = executeUpdate(conn, sqlMapUnmatchedToPdm());
        int secondMatched = executeUpdate(conn, sqlMapOwnerBySubj());
        int unmatchedMarked = executeUpdate(conn, sqlMarkUnmatched());

        OwnerAggregationResult result = countByStatus(conn);
        result.truncated = truncated;
        result.inserted = inserted;
        result.prefixUpdated = prefixUpdated;
        result.firstMatched = firstMatched;
        result.ddlMapped = ddlMapped;
        result.pdmMapped = pdmMapped;
        result.secondMatched = secondMatched;
        result.unmatchedMarked = unmatchedMarked;

        logResult(result);
        return result;
    }

    private int truncateAggregateTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE wat_cust_tbl_owner");
            return 1;
        }
    }

    private String sqlInsertDistinctTables() {
        return "INSERT INTO wat_cust_tbl_owner ("
                + " db_nm, schema_id, schema_nm, ddl_tbl_pnm"
                + ") SELECT"
                + " c.db_nm,"
                + " MAX(c.schema_id) AS schema_id,"
                + " c.schema_nm,"
                + " c.ddl_tbl_pnm"
                + " FROM wam_cust_info c"
                + " GROUP BY c.db_nm, c.schema_nm, c.ddl_tbl_pnm";
    }

    private String sqlUpdateTablePrefix() {
        return "UPDATE wat_cust_tbl_owner t"
                + " SET t.tbl_prefix_cd = UPPER(SUBSTRING(t.ddl_tbl_pnm, 1, 3))"
                + " WHERE t.tbl_prefix_cd IS NULL";
    }

    private String sqlMapOwnerByUniqueModuleCode() {
        return "UPDATE wat_cust_tbl_owner t"
                + " JOIN ("
                + "  SELECT s.module_cd, s.subj_id, s.tbl_owner_id, s.tbl_owner_nm"
                + "    FROM waa_subj s"
                + "    JOIN ("
                + "          SELECT module_cd"
                + "            FROM waa_subj"
                + "           WHERE module_cd IS NOT NULL"
                + "             AND module_cd <> ''"
                + "           GROUP BY module_cd"
                + "          HAVING COUNT(*) = 1"
                + "    ) uniq ON uniq.module_cd = s.module_cd"
                + " ) u ON u.module_cd = t.tbl_prefix_cd"
                + " SET t.subj_id = u.subj_id,"
                + "     t.tbl_owner_id = u.tbl_owner_id,"
                + "     t.tbl_owner_nm = u.tbl_owner_nm"
                + " WHERE t.tbl_owner_id IS NULL";
    }

    private String sqlMapUnmatchedToDdl() {
        return "UPDATE wat_cust_tbl_owner t"
                + " JOIN wam_ddl_tbl d"
                + "   ON d.db_nm = t.db_nm"
                + "  AND d.schema_nm = t.schema_nm"
                + "  AND d.ddl_tbl_pnm = t.ddl_tbl_pnm"
                + " SET t.ddl_tbl_id = d.ddl_tbl_id,"
                + "     t.ddl_pdm_id = d.ddl_pdm_id"
                + " WHERE t.tbl_owner_id IS NULL";
    }

    private String sqlMapUnmatchedToPdm() {
        return "UPDATE wat_cust_tbl_owner t"
                + " JOIN wam_pdm_tbl p"
                + "   ON p.pdm_tbl_id = t.ddl_pdm_id"
                + " SET t.pdm_tbl_id = p.pdm_tbl_id,"
                + "     t.subj_id = COALESCE(t.subj_id, p.subj_id)"
                + " WHERE t.tbl_owner_id IS NULL"
                + "   AND t.ddl_pdm_id IS NOT NULL";
    }

    private String sqlMapOwnerBySubj() {
        return "UPDATE wat_cust_tbl_owner t"
                + " JOIN waa_subj s"
                + "   ON s.subj_id = t.subj_id"
                + " SET t.tbl_owner_id = s.tbl_owner_id,"
                + "     t.tbl_owner_nm = s.tbl_owner_nm"
                + " WHERE t.tbl_owner_id IS NULL"
                + "   AND t.subj_id IS NOT NULL";
    }

    private String sqlMarkUnmatched() {
        return "UPDATE wat_cust_tbl_owner t"
                + " SET t.tbl_owner_id = 'UNMATCHED',"
                + "     t.tbl_owner_nm = 'UNMATCHED'"
                + " WHERE t.tbl_owner_id IS NULL";
    }

    private int executeUpdate(Connection conn, String sql) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            return ps.executeUpdate();
        }
    }

    private OwnerAggregationResult countByStatus(Connection conn) throws SQLException {
        OwnerAggregationResult result = new OwnerAggregationResult();
        try (Statement stmt = conn.createStatement()) {
            try (java.sql.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM wat_cust_tbl_owner")) {
                if (rs.next()) result.total = rs.getInt(1);
            }
            try (java.sql.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM wat_cust_tbl_owner WHERE tbl_owner_id = 'UNMATCHED'")) {
                if (rs.next()) result.unmatched = rs.getInt(1);
            }
            try (java.sql.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM wat_cust_tbl_owner WHERE tbl_owner_id IS NOT NULL AND tbl_owner_id <> 'UNMATCHED'")) {
                if (rs.next()) result.matched = rs.getInt(1);
            }
        }
        return result;
    }

    private void logResult(OwnerAggregationResult r) {
        System.out.println("[OwnerAgg] truncate=" + r.truncated + ", insert=" + r.inserted + ", prefix=" + r.prefixUpdated
                + ", firstMatch=" + r.firstMatched + ", ddlMap=" + r.ddlMapped + ", pdmMap=" + r.pdmMapped
                + ", secondMatch=" + r.secondMatched + ", unmatchedMark=" + r.unmatchedMarked);
        System.out.println("[OwnerAgg] total=" + r.total + ", matched=" + r.matched + ", unmatched=" + r.unmatched);
    }

    public static class OwnerAggregationResult {
        public int truncated;
        public int inserted;
        public int prefixUpdated;
        public int firstMatched;
        public int ddlMapped;
        public int pdmMapped;
        public int secondMatched;
        public int unmatchedMarked;
        public int total;
        public int matched;
        public int unmatched;
    }
}
