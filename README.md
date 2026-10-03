# data2flow-ingress

외부 MQTT 브로커를 **구독만** 해서 받은 원본을 RabbitMQ Super Stream `data2flow.raw`에 기록하고, **publisher confirm을 받은 뒤에만** 브로커에 확인(PUBACK)하는 수집 서비스입니다. 수집 경로를 만들거나 운영하는 백엔드 개발자가 읽습니다. 다 읽으면 로컬에서 실행하고, 설정 키와 비밀값을 넣고, 무손실 시험을 돌릴 수 있습니다.

- 관련 스펙: ING-01.01·01.03, DSC-01.02·01.04·01.05·02.01~02.06·07.01·07.03·09.02~09.04·09.10·09.11, NFR-01.01~01.04·02.02~02.04·02.09, OPS-01.02·02.03 (정본은 비공개 저장소 `data2flow-docs`)
- 패키지: `net.java21.data2flow.ingress` · Spring Boot 4.1.1 · Java 21 · Maven Wrapper
- 포트: API 8080(내부 전용), actuator 8081(프로브·지표·소스별 연결 상태)

## 1. 하는 일

```
외부 브로커(MQTT 3.1.1/5, tcp·ssl·ws·wss) ─구독(QoS 1, 영속 세션)→ MqttConnectorSession ─RawEnvelope v1→ RawStreamWriter ─confirm→ PUBACK
        ▲ core-api API-DSC-50(소스 설정) + data2flow.config(설정 변경)        └→ data2flow.events: EVT-DSC-02(상태)·03(1분 통계)·09(카탈로그)
```

| 기능 | 동작 | 스펙 |
|---|---|---|
| 무손실 수신 | 스트림 confirm 뒤에만 받은 순서대로 PUBACK. 기록 실패·confirm 10초 초과면 확인하지 않고 끊은 뒤 영속 세션으로 재접속(브로커 재전송) | DSC-09.03, BR-DSC-24 |
| 이중 수신 | 인스턴스 2개가 서로 다른 client-id로 같은 토픽을 구독(DUAL_ACTIVE). 중복은 같은 `dedupKey`라 pipeline이 거른다. MQTT 5 + `sharedGroup`이면 공유 구독(SCALABLE) | ADR-015, BR-DSC-25 |
| client-id | `{base}-{env}-{n}`(운영 `data2flow-ingress-prod-0`), 개발자 `{base}-dev-{이름}-{n}`. `n`은 파드 이름 끝 번호(StatefulSet) | BR-DSC-01, CLAUDE.md §5 |
| 재연결 | 1, 2, 4 … 최대 60초. 인증·TLS 오류 5회면 ERROR, 5분마다 재시도 | DSC-02.02 |
| 역압 | MQTT 5 receive maximum(기본 100), 스트림 미확인 한도(2,000). 기록이 밀리면 확인이 늦어지고 브로커가 보내기를 멈춘다 | reliability-and-ha.md §5 |
| 일시정지 | PAUSED는 세션을 남긴 채 끊고, 재개하면 쌓인 메시지부터 받는다 | DSC-07.01 |
| 종료 | SIGTERM → 새 메시지는 넘기지 않고, 기록 중인 것의 confirm·PUBACK을 기다린 뒤(최대 20초) 세션을 남긴 채 끊는다 | reliability-and-ha.md §4.1 |
| 연결 테스트 | `POST /internal/ingress/sources/test`(API-DSC-51): DNS→TCP→TLS→AUTH→SUBSCRIBE, 미리보기 10건, 최대 30초, 조직당 동시 3개 | DSC-02.05·09.11 |
| 실시간 보기 | `GET /internal/ingress/sources/{source-id}/live`(API-DSC-52, SSE): 초당 10건, 넘치면 `dropped` | DSC-02.06 |
| 관측 | readiness = `data2flow.raw` 생산자 준비. 지표 `data2flow_ingress_*`, 소스별 상태 `/actuator/health`의 `sources` | OPS-01.02 |

**구독 전용입니다.** 운영 코드에는 MQTT 발행 경로가 없고(`ArchitectureTest`가 HiveMQ 발행 API 호출을 막습니다), 송신 API(API-DSC-61)는 만들지 않았습니다(ACT-03.02 결정 대기).

## 2. 빌드와 실행

```bash
./mvnw verify                 # 단위(*Test) + 통합(*IT, Testcontainers) + 커버리지 80% 검사
./mvnw spring-boot:run        # 로컬 실행(프로필 local)
```

`*IT`는 Docker가 필요합니다. Mosquitto 2.0·RabbitMQ 3.13(stream)·nginx·Toxiproxy를 임시 컨테이너로 띄우고 공용 인프라(s3·s4·iot-data)에는 붙지 않습니다. 무손실 시험 결과는 `target/lossless-report.txt`에 남습니다.

공통 라이브러리 `data2flow-contracts`는 GitHub Packages에 있어서 읽기에도 토큰이 필요합니다. `~/.m2/settings.xml`에 서버 `github`(사용자 이름 + `read:packages` 권한 토큰)를 넣거나, `data2flow-contracts`를 받아 `./mvnw install`로 로컬 저장소에 설치합니다.

### 로컬 실행 순서

1. s3 SSH 터널로 RabbitMQ를 `localhost:5672`·`5552`에 엽니다(`data2flow-docs/design/deployment.md` §8.2).
2. 루트 `.env`의 값을 환경변수로 넣습니다(아래 표의 키). `DATA2FLOW_DEV_NAME`은 반드시 넣습니다. client-id가 `data2flow-ingress-dev-{이름}-0`이 되어 다른 사람·운영 연결을 끊지 않습니다.
3. core-api를 로컬에서 띄우고 `DATA2FLOW_CORE_URI`로 가리킵니다(API-DSC-50). 소스 설정과 비밀값은 core가 줍니다.

## 3. 설정

비밀값은 환경변수로만 받습니다(로컬은 루트 `.env`, 운영은 k8s Secret). 표의 값은 기본값이고 실제 비밀값은 적지 않습니다.

| 설정 키 | 환경변수(.env 키) | 기본값 | 설명 |
|---|---|---|---|
| `data2flow.ingress.instance-id` | `HOSTNAME` | `ingress-local-0` | 파드 이름. `RawEnvelope.ingressInstance`, 끝 번호가 client-id 번호 |
| `data2flow.ingress.env` | `DATA2FLOW_INGRESS_ENV` | 프로필별 `dev`·`stg`·`prod` | client-id 환경 이름 |
| `data2flow.ingress.developer` | `DATA2FLOW_DEV_NAME` | (없음) | 로컬 개발자 이름. 있으면 client-id `{base}-dev-{이름}-{n}` |
| `data2flow.ingress.core-uri` | `DATA2FLOW_CORE_URI` | `http://data2flow-core-api`(local `http://localhost:8082`) | core-api 내부 주소 |
| `data2flow.ingress.credentials.iot-data-basic` | `MQTT_BASIC_AUTH` | (없음) | `auth.credentialRef = secret://sources/iot-data/basic`이고 core가 비밀값을 주지 않을 때만 쓰는 `사용자:비밀번호` |
| `data2flow.ingress.stream.host`·`port` | `DATA2FLOW_RABBITMQ_HOST`, `DATA2FLOW_RABBITMQ_STREAM_PORT` | `localhost`, `5552` | RabbitMQ Stream |
| `data2flow.ingress.stream.virtual-host` | `DATA2FLOW_RABBITMQ_VHOST` | `data2flow-dev` | prod `data2flow`, staging `data2flow-stg`(ADR-030) |
| `data2flow.ingress.stream.username`·`password` | `DATA2FLOW_RABBITMQ_USERNAME`, `DATA2FLOW_RABBITMQ_PASSWORD` | `guest` | Stream 계정 |
| `data2flow.ingress.stream.use-configured-address` | – | local `true` | 서버가 알려 주는 주소 대신 항상 host:port로 접속(SSH 터널) |
| `spring.rabbitmq.host`·`port`·`virtual-host`·`username`·`password` | `DATA2FLOW_RABBITMQ_HOST`, `DATA2FLOW_RABBITMQ_PORT`, `DATA2FLOW_RABBITMQ_VHOST`, `DATA2FLOW_RABBITMQ_USERNAME`, `DATA2FLOW_RABBITMQ_PASSWORD` | `localhost:5672` | AMQP(설정 변경 수신, 상태 이벤트) |
| `data2flow.ingress.source-filter.organization-ids` | `DATA2FLOW_INGRESS_ORGANIZATION_IDS` | (전부) | 실행할 조직. staging은 staging 전용 조직만 |
| `data2flow.ingress.source-filter.denied-hosts` | `DATA2FLOW_INGRESS_DENIED_HOSTS` | staging `iot-data.java21.net` | 구독하지 않을 브로커 호스트(staging은 실제 외부 소스 구독 금지) |
| `data2flow.ingress.mqtt.confirm-timeout` | – | `10s` | 넘으면 확인하지 않고 재접속 |
| `data2flow.ingress.mqtt.receive-maximum` | – | `100` | MQTT 5 동시 미확인 한도 |
| `data2flow.ingress.mqtt.backoff-initial`·`backoff-max` | – | `1s`·`60s` | 재연결 간격 |
| `data2flow.ingress.drain-timeout` | – | `20s` | 종료 때 confirm 대기 |
| `data2flow.ingress.resync-interval` | – | `5m` | core 설정 전체 재동기화 주기 |
| `data2flow.ingress.report-interval`·`stats-interval` | – | `30s`·`1m` | EVT-DSC-02·03 주기 |
| `data2flow.ingress.connection-test.max-per-organization` | – | `3` | 조직당 동시 연결 테스트 |
| `management.tracing.sampling.probability` | `DATA2FLOW_TRACING_SAMPLING` | `0.1` | 추적 표본 비율(OTLP 내보내기는 주소를 정한 환경만) |

### 소스 설정(core-api가 주는 `config`)

MQTT 커넥터(키 `mqtt`) 설정 스키마는 `src/main/resources/connectors/mqtt.schema.json`이고 카탈로그 보고(EVT-DSC-09)에 실립니다. DSC 도메인 모델 §2.2의 평평한 모양(`url, qos, keepaliveSec, cleanStart, sessionExpirySec, auth: NONE|USERPASS|HEADER|MTLS, headerName, headerScheme`)과 connectors.md §3 예시 모양(`version, subscriptions[], session{}, auth{type: ws-header, scheme}, tls{verify}`)을 모두 읽습니다. 비밀값 종류는 `PASSWORD`, `HEADER_VALUE`(Basic이면 `사용자:비밀번호`도 받음), `CA_CERT`, `CLIENT_CERT`, `CLIENT_KEY`(PKCS#8 PEM)입니다.

## 4. 시험

| 시험 | 내용 |
|---|---|
| `Mqtt311·Mqtt5·MqttWssBasic·MqttMtlsConnectorContractIT` | 커넥터 계약 키트(TC-DSC-237~240). 확인 수는 프록시가 실제 PUBACK 패킷으로 센다 |
| `DualIngressKill9IT` | NFR-02.09: 별도 JVM 2개, 초당 200건 4,000건 중 1대 `kill -9`(SIGKILL) → 유실 0·중복 제거 후 중복 0·수신 공백 0 |
| `IngressKillBeforeConfirmIT` | TC-ING-017: confirm 3초 지연 중 `kill -9` → 같은 client-id로 재시작, 300건 유실 0 |
| `GracefulShutdownIT` | TC-ING-021: SIGTERM 때 기록 중 메시지 confirm·PUBACK 후 종료, 유실 0·중복 0 |
| `MqttManualAckIT` | TC-ING-016: Stream이 끊긴 동안 PUBACK 0, 복구 후 재전송으로 모두 기록 |
| `ReconnectBackoffIT`, `ConnectorScalingIT`, `StagedConnectionTestIT`, `IngressCollectionIT` | 재연결 1·2·4·8초, 공유 구독 1만 건 분배·이중 수신, 단계별 연결 테스트, 수집 경로 전체 |

## 5. 작업 규칙

스펙 ID에서 시작하고(인수 테스트 → 테스트 케이스 → 구현), 브랜치·PR·테스트 이름에 스펙 ID를 남깁니다. 1.0 전에는 `main` + `feat/<스펙ID>-<요약>`, 1.0 뒤에는 버전 브랜치 `feature/vX.Y`를 씁니다(ADR-039).
