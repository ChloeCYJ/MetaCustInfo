package mng;

import dao.CustInfoAllFileWriterDaoNew;
import dao.CustInfoTableOwnerAggDaoNew;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * 기존 배치 관리 클래스.
 *
 * <p>요구사항: 기존 CustInfoAllFileWriterDaoNew 호출 전에
 * CustInfoTableOwnerAggDaoNew.executeOwnerAggregation()를 먼저 실행한다.
 */
public class CustInfoAllFileWriterMngNew {

    private final CustInfoTableOwnerAggDaoNew ownerAggDao = new CustInfoTableOwnerAggDaoNew();
    private final CustInfoAllFileWriterDaoNew fileWriterDao = new CustInfoAllFileWriterDaoNew();

    public void run() throws Exception {
        String url = System.getenv().getOrDefault("DB_URL", "jdbc:mariadb://127.0.0.1:3306/meta");
        String user = System.getenv().getOrDefault("DB_USER", "meta_user");
        String password = System.getenv().getOrDefault("DB_PASSWORD", "meta_password");

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            conn.setAutoCommit(false);
            try {
                ownerAggDao.executeOwnerAggregation(conn); // 선행 호출
                fileWriterDao.execute(conn);               // 기존 호출
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
    }
}
