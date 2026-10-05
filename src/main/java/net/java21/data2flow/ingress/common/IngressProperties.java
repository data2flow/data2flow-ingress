package net.java21.data2flow.ingress.common;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ingress 설정({@code data2flow.ingress.*}). 키와 환경변수 이름은 README "설정"에 있다. 비밀값(비밀번호)은 환경변수로만 받는다.
 *
 * @param instanceId     파드 이름({@code RawEnvelope.ingressInstance}, 상태 보고 instanceId). k8s는 HOSTNAME
 * @param instanceOrdinal client-id 끝 번호. 음수면 instanceId 끝의 {@code -숫자}(StatefulSet 순번), 없으면 0
 * @param env            client-id 환경 이름: {@code prod}, {@code stg}, {@code dev}(BR-DSC-01)
 * @param developer      로컬 개발자 이름. 있으면 client-id가 {@code {base}-dev-{developer}-{n}}(deployment.md §8.2)
 * @param coreUri        core-api 내부 주소(API-DSC-50)
 * @param resyncInterval 설정 전체를 다시 읽는 주기(설정 변경 메시지를 놓쳐도 이 안에 맞춘다)
 * @param reportInterval 연결 상태 보고 주기(EVT-DSC-02, 30초)
 * @param statsInterval  수신 통계 보고 주기(EVT-DSC-03, 1분)
 * @param drainTimeout   종료 때 기록 중인 메시지 confirm을 기다리는 최대 시간(reliability-and-ha.md §4.1, 20초)
 * @param autoStart      시작할 때 core-api에서 설정을 읽어 연결을 시작한다(테스트에서 끈다)
 * @param sourceFilter   이 인스턴스가 실행할 소스 제한(staging은 실제 외부 소스를 구독하지 않는다, deployment.md §9)
 * @param credentials    {@code auth.credentialRef}가 가리키는 비밀값(이름 → 값). core가 비밀값을 주지 않은 소스에만 쓴다
 * @param mqtt           MQTT 커넥터 기본값
 * @param stream         {@code data2flow.raw} 스트림 접속
 * @param connectionTest 연결 테스트(API-DSC-51)
 * @param live           원본 실시간 보기(API-DSC-52)
 * @param platformBroker 플랫폼 브로커 접속(DSC-03.01, ADR-029). PLATFORM_BROKER 소스 설정에는 주소가 없어 이 값을 쓴다
 * @param signing        플랫폼 브로커 기기 서명 키 캐시(DSC-03.02·03.03, ADR-042)
 * @param db             폴링 위치·리더 리스·Webhook 재생 방지 저장소(스키마 {@code data2flow_ingress}, ADR-052). url이 비면 메모리
 * @param lease          SINGLETON 커넥터 리더 리스(BR-DSC-26)
 * @param polling        폴링 커넥터 공통(DSC-09.09)
 * @param webhook        Webhook 수신(DSC-01.03)
 * @param payload        payload 형식 변환·스키마 조회(DSC-09.07)
 */
@ConfigurationProperties("data2flow.ingress")
public record IngressProperties(
        @DefaultValue("ingress-local-0") String instanceId,
        @DefaultValue("-1") int instanceOrdinal,
        @DefaultValue("dev") String env,
        String developer,
        @DefaultValue("http://data2flow-core-api") String coreUri,
        @DefaultValue("5m") Duration resyncInterval,
        @DefaultValue("30s") Duration reportInterval,
        @DefaultValue("1m") Duration statsInterval,
        @DefaultValue("20s") Duration drainTimeout,
        @DefaultValue("true") boolean autoStart,
        @DefaultValue SourceFilter sourceFilter,
        Map<String, String> credentials,
        @DefaultValue Mqtt mqtt,
        @DefaultValue Stream stream,
        @DefaultValue ConnectionTest connectionTest,
        @DefaultValue Live live,
        @DefaultValue PlatformBroker platformBroker,
        @DefaultValue Signing signing,
        @DefaultValue Db db,
        @DefaultValue Lease lease,
        @DefaultValue Polling polling,
        @DefaultValue Webhook webhook,
        @DefaultValue Payload payload) {

    private static final Pattern TRAILING_ORDINAL = Pattern.compile(".*-(\\d+)$");

    public IngressProperties {
        credentials = credentials == null ? Map.of() : Map.copyOf(credentials);
    }

    /** client-id 끝 번호(BR-DSC-01 instanceOrdinal) */
    public int ordinal() {
        if (instanceOrdinal >= 0) {
            return instanceOrdinal;
        }
        Matcher m = TRAILING_ORDINAL.matcher(instanceId);
        return m.matches() ? Integer.parseInt(m.group(1)) : 0;
    }

    /**
     * @param organizationIds 실행할 조직(비면 전부). staging은 전용 조직만(ADR-030)
     * @param deniedHosts     접속하지 않을 브로커 호스트. staging은 {@code iot-data.java21.net}(실제 외부 소스 구독 금지)
     */
    public record SourceFilter(List<Long> organizationIds, List<String> deniedHosts) {
        public SourceFilter {
            organizationIds = organizationIds == null ? List.of() : List.copyOf(organizationIds);
            deniedHosts = deniedHosts == null ? List.of() : List.copyOf(deniedHosts);
        }
    }

    /**
     * @param clientIdBase       소스가 client-id base를 주지 않을 때 쓰는 값
     * @param backoffInitial     재연결 첫 간격(DSC-02.02: 1초)
     * @param backoffMax         재연결 최대 간격(60초)
     * @param fatalAttempts      인증·TLS처럼 다시 해도 실패가 확실한 오류를 이 횟수만큼 겪으면 ERROR(5회)
     * @param fatalRetryInterval ERROR 상태의 재시도 간격(5분)
     * @param confirmTimeout     스트림 confirm을 기다리는 시간. 넘으면 MQTT를 끊고 영속 세션으로 다시 접속(reliability-and-ha.md ②, 10초)
     * @param receiveMaximum     MQTT 5 동시 미확인 메시지 한도(역압). MQTT 3.1.1은 브로커 max_inflight가 정한다
     * @param maxPayloadBytes    이 크기를 넘는 payload는 앞부분만 기록한다(스트림 프레임 한도 보호). pipeline이 INVALID(SIZE_LIMIT)로 분류(BR-DSC-06)
     */
    public record Mqtt(@DefaultValue("data2flow-ingress") String clientIdBase,
                       @DefaultValue("1s") Duration backoffInitial,
                       @DefaultValue("60s") Duration backoffMax,
                       @DefaultValue("5") int fatalAttempts,
                       @DefaultValue("5m") Duration fatalRetryInterval,
                       @DefaultValue("10s") Duration confirmTimeout,
                       @DefaultValue("100") int receiveMaximum,
                       @DefaultValue("524288") int maxPayloadBytes) {
    }

    /**
     * @param host                  RabbitMQ Stream 호스트
     * @param port                  Stream 포트(5552)
     * @param virtualHost           vhost(prod {@code data2flow}, staging {@code data2flow-stg}, 로컬 {@code data2flow-dev})
     * @param username              사용자
     * @param password              비밀번호(환경변수)
     * @param useConfiguredAddress  서버가 알려 주는 주소 대신 항상 host:port로 접속(로컬 SSH 터널·테스트, deployment.md §8.2)
     * @param createSuperStream     없으면 {@code SuperStreamSpec.RAW}로 만든다
     * @param maxUnconfirmed        confirm을 기다리는 메시지 한도(넘으면 발행이 기다린다 = 역압)
     */
    public record Stream(@DefaultValue("localhost") String host,
                         @DefaultValue("5552") int port,
                         @DefaultValue("data2flow-dev") String virtualHost,
                         @DefaultValue("guest") String username,
                         @DefaultValue("guest") String password,
                         @DefaultValue("false") boolean useConfiguredAddress,
                         @DefaultValue("true") boolean createSuperStream,
                         @DefaultValue("2000") int maxUnconfirmed) {
    }

    /**
     * @param defaultTimeout   요청이 시간을 주지 않을 때(BR-DSC-07: 15초)
     * @param maxTimeout       요청 상한(API-DSC-51: 30초)
     * @param maxPerOrganization 조직당 동시 테스트 수(API-DSC-51: 3)
     */
    public record ConnectionTest(@DefaultValue("15s") Duration defaultTimeout,
                                 @DefaultValue("30s") Duration maxTimeout,
                                 @DefaultValue("3") int maxPerOrganization) {
    }

    /**
     * @param maxRatePerSecond 구독자 하나에 보내는 초당 최대 건수(API-DSC-10 기본 10/s). 넘으면 버리고 dropped로 알린다
     * @param maxSubscribers   인스턴스 전체 동시 구독 한도
     * @param timeout          SSE 연결 최대 유지 시간
     */
    public record Live(@DefaultValue("10") int maxRatePerSecond,
                       @DefaultValue("20") int maxSubscribers,
                       @DefaultValue("30m") Duration timeout) {
    }

    /**
     * 플랫폼 브로커(DSC-03.01, ADR-029). 운영은 공용 {@code wss://iot-data.java21.net/mqtt}을 <b>구독만</b> 한다.
     *
     * @param url      브로커 주소(tcp·ssl·ws·wss)
     * @param topics   구독 토픽(쉼표 목록 가능). 기본 {@code devices/+/telemetry}(BR-DSC-12)
     * @param version  MQTT 버전(5.0, 3.1.1)
     * @param username 접속 사용자(선택). 비밀번호와 함께 주면 ws·wss는 Basic 헤더, tcp·ssl은 사용자/비밀번호로 접속
     * @param password 접속 비밀번호(선택, 환경변수로만)
     */
    public record PlatformBroker(@DefaultValue("wss://iot-data.java21.net/mqtt") String url,
                                 @DefaultValue("devices/+/telemetry") List<String> topics,
                                 @DefaultValue("5.0") String version,
                                 String username,
                                 String password) {
        public PlatformBroker {
            topics = topics == null ? List.of() : List.copyOf(topics);
        }

        @Override
        public String toString() {
            return "PlatformBroker[url=" + url + ", topics=" + topics + ", version=" + version + ", username=" + username + "]";
        }
    }

    /**
     * @param refreshInterval 서명 키 전체를 다시 읽는 주기. 폐기가 1분 안에 반영되도록 60초 이하여야 한다(DSC-03.02)
     */
    public record Signing(@DefaultValue("30s") Duration refreshInterval) {
        public Signing {
            if (refreshInterval == null || refreshInterval.isZero() || refreshInterval.isNegative()
                    || refreshInterval.compareTo(Duration.ofSeconds(60)) > 0) {
                throw new IllegalArgumentException("data2flow.ingress.signing.refresh-interval은 0초 초과 60초 이하여야 합니다(DSC-03.02)");
            }
        }
    }

    /**
     * ingress 전용 저장소(ADR-052: {@code data2flow_ingress.source_poll_cursors}·{@code connector_leases}·{@code webhook_requests}).
     *
     * @param url         JDBC 주소. 비면 저장소 없이 메모리(인스턴스 하나·재시작하면 처음부터, 로컬 개발용)
     * @param username    사용자
     * @param password    비밀번호(환경변수)
     * @param flywayMode  {@code migrate}(staging만, expand 마이그레이션), {@code validate}(prod·local), {@code none}(ADR-030)
     * @param maxPoolSize 연결 풀 크기
     */
    public record Db(String url, String username, String password, @DefaultValue("validate") String flywayMode,
                     @DefaultValue("4") int maxPoolSize) {
        public boolean enabled() {
            return url != null && !url.isBlank();
        }

        @Override
        public String toString() {
            return "Db[url=" + url + ", username=" + username + ", flywayMode=" + flywayMode + "]";
        }
    }

    /**
     * @param ttl        리스 길이(ConnectorLease.TTL 30초)
     * @param renewEvery 갱신·대기 소스 리스 시도 주기(10초). 리더가 죽으면 ttl + renewEvery(40초) 안에 다른 인스턴스가 넘겨받는다(60초 이내)
     */
    public record Lease(@DefaultValue("30s") Duration ttl, @DefaultValue("10s") Duration renewEvery) {
        public Lease {
            if (renewEvery.compareTo(ttl) >= 0) {
                throw new IllegalArgumentException("data2flow.ingress.lease.renew-every는 ttl보다 짧아야 합니다(BR-DSC-26)");
            }
        }
    }

    /** @param minInterval 사용자 폴링 주기의 하한(PollingPolicy.MIN_INTERVAL 10초) */
    public record Polling(@DefaultValue("10s") Duration minInterval) {
    }

    /** @param writeTimeout 스트림 기록 confirm을 기다리는 시간. 넘으면 503(상대가 다시 보냄) */
    public record Webhook(@DefaultValue("30s") Duration writeTimeout) {
    }

    /**
     * @param maxDecompressedBytes 압축을 푼 payload 한도(압축 폭탄 방지, 1MiB). 넘으면 DECODE_ERROR
     * @param schemaTimeout        업로드 스키마(core API-DSC-81)·Avro 레지스트리 조회 제한 시간
     */
    public record Payload(@DefaultValue("1048576") int maxDecompressedBytes,
                          @DefaultValue("5s") Duration schemaTimeout) {
    }
}
