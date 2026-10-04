package net.java21.data2flow.ingress.support;

import com.zaxxer.hikari.HikariDataSource;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.config.PersistenceConfig;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * 시험 전용 PostgreSQL 18(운영 s3 공용 DB와 같은 주 버전). 공용 DB에는 붙지 않는다(CLAUDE.md §5). 스키마 {@code data2flow_ingress}는
 * 운영과 같은 Flyway 마이그레이션으로 만든다.
 */
public final class PostgresTestDb {

    private static PostgreSQLContainer shared;

    private PostgresTestDb() {
    }

    public static synchronized PostgreSQLContainer shared() {
        if (shared == null) {
            shared = new PostgreSQLContainer("postgres:18-alpine").withDatabaseName("data2flow");
            shared.start();
        }
        return shared;
    }

    /** Flyway migrate까지 한 연결 풀(PersistenceConfig와 같은 경로) */
    public static HikariDataSource dataSource() {
        PostgreSQLContainer pg = shared();
        IngressProperties p = IngressFixtures.props("dev", null, java.util.List.of(), java.util.List.of(), java.util.Map.of());
        IngressProperties withDb = new IngressProperties(p.instanceId(), p.instanceOrdinal(), p.env(), p.developer(),
                p.coreUri(), p.resyncInterval(), p.reportInterval(), p.statsInterval(), p.drainTimeout(), p.autoStart(),
                p.sourceFilter(), p.credentials(), p.mqtt(), p.stream(), p.connectionTest(), p.live(), p.platformBroker(),
                p.signing(), new IngressProperties.Db(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword(), "migrate", 4),
                p.lease(), p.polling(), p.webhook());
        return new PersistenceConfigAccess().dataSource(withDb);
    }

    /** PersistenceConfig의 패키지 밖에서 빈 메서드를 부른다 */
    private static final class PersistenceConfigAccess extends PersistenceConfig {
        HikariDataSource dataSource(IngressProperties properties) {
            return super.ingressDataSourceForTest(properties);
        }
    }
}
