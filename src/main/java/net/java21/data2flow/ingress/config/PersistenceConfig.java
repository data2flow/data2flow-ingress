package net.java21.data2flow.ingress.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.java21.data2flow.ingress.common.IngressProperties;
import net.java21.data2flow.ingress.lease.repository.LeaseRepository;
import net.java21.data2flow.ingress.lease.repository.WebhookRequestRepository;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Locale;

/**
 * ingress 전용 저장소(ADR-052, 스키마 {@code data2flow_ingress}): 폴링 위치·리더 리스·Webhook 재생 방지. {@code data2flow.ingress.db.url}이
 * 있을 때만 켠다(로컬은 메모리). Flyway는 staging만 {@code migrate}, prod·local은 {@code validate}(ADR-030).
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnExpression("!'${data2flow.ingress.db.url:}'.isBlank()")
public class PersistenceConfig {

    public static final String SCHEMA = "data2flow_ingress";
    private static final Logger log = LoggerFactory.getLogger(PersistenceConfig.class);

    @Bean(destroyMethod = "close")
    HikariDataSource ingressDataSource(IngressProperties properties) {
        return ingressDataSourceForTest(properties);
    }

    /** 연결 풀을 만들고 Flyway를 적용한다(시험도 같은 경로를 쓴다) */
    protected HikariDataSource ingressDataSourceForTest(IngressProperties properties) {
        IngressProperties.Db db = properties.db();
        HikariConfig c = new HikariConfig();
        c.setJdbcUrl(db.url());
        c.setUsername(db.username());
        c.setPassword(db.password());
        c.setMaximumPoolSize(db.maxPoolSize());
        c.setPoolName("ingress-db");
        c.setConnectionTimeout(5_000);
        c.addDataSourceProperty("ApplicationName", "data2flow-ingress");
        HikariDataSource ds = new HikariDataSource(c);
        Flyway flyway = Flyway.configure().dataSource(ds).schemas(SCHEMA).createSchemas(true)
                .locations("classpath:db/migration").load();
        switch (db.flywayMode().toLowerCase(Locale.ROOT)) {
            case "migrate" -> log.info("Flyway migrate: {}건 적용", flyway.migrate().migrationsExecuted);
            case "validate" -> flyway.validate();
            case "none" -> log.info("Flyway 건너뜀(data2flow.ingress.db.flyway-mode=none)");
            default -> throw new IllegalArgumentException("data2flow.ingress.db.flyway-mode는 migrate·validate·none입니다");
        }
        return ds;
    }

    @Bean
    JdbcTemplate ingressJdbcTemplate(HikariDataSource ingressDataSource) {
        return new JdbcTemplate(ingressDataSource);
    }

    @Bean
    LeaseRepository leaseRepository(JdbcTemplate ingressJdbcTemplate) {
        return new LeaseRepository(ingressJdbcTemplate);
    }

    @Bean
    WebhookRequestRepository webhookRequestRepository(JdbcTemplate ingressJdbcTemplate) {
        return new WebhookRequestRepository(ingressJdbcTemplate);
    }
}
