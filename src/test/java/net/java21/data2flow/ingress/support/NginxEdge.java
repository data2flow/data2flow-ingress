package net.java21.data2flow.ingress.support;

import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import java.time.Duration;

/**
 * iot-data.java21.net 구성(research/02-dev-environment.md §2.3)을 흉내 낸 테스트 nginx.
 * <ul>
 *   <li>8443: TLS + HTTP Basic 인증 → MQTT over WebSocket(호스트의 확인 계수 프록시 → Mosquitto 9001). Authorization 헤더는 넘기지 않는다.</li>
 *   <li>8883: TLS + 클라이언트 인증서 검증(mTLS) → MQTT tcp(호스트 프록시 → Mosquitto 1883).</li>
 * </ul>
 */
public final class NginxEdge {

    public static final int WSS_PORT = 8443;
    public static final int MTLS_PORT = 8883;

    private NginxEdge() {
    }

    /**
     * @param wsProxyPort  WebSocket 프록시 포트(0이면 WSS 서버 없음)
     * @param tcpProxyPort tcp 프록시 포트(0이면 mTLS 서버 없음)
     */
    public static GenericContainer<?> start(TestPki pki, int wsProxyPort, int tcpProxyPort, String user, String password) {
        StringBuilder conf = new StringBuilder("error_log /dev/stderr notice;\nevents {}\n");
        if (wsProxyPort > 0) {
            Testcontainers.exposeHostPorts(wsProxyPort);
            conf.append("""
                    http {
                      server {
                        listen 8443 ssl;
                        server_name localhost;
                        ssl_certificate /etc/nginx/certs/server.pem;
                        ssl_certificate_key /etc/nginx/certs/server-key.pem;
                        ssl_protocols TLSv1.2 TLSv1.3;
                        location /mqtt {
                          auth_basic "iot-data mqtt";
                          auth_basic_user_file /etc/nginx/htpasswd;
                          proxy_pass http://host.testcontainers.internal:%d;
                          proxy_http_version 1.1;
                          proxy_set_header Upgrade $http_upgrade;
                          proxy_set_header Connection "upgrade";
                          proxy_set_header Authorization "";
                          proxy_read_timeout 1h;
                          proxy_send_timeout 1h;
                          proxy_buffering off;
                        }
                        location / { return 404; }
                      }
                    }
                    """.formatted(wsProxyPort));
        }
        if (tcpProxyPort > 0) {
            Testcontainers.exposeHostPorts(tcpProxyPort);
            conf.append("""
                    stream {
                      server {
                        listen 8883 ssl;
                        ssl_certificate /etc/nginx/certs/server.pem;
                        ssl_certificate_key /etc/nginx/certs/server-key.pem;
                        ssl_client_certificate /etc/nginx/certs/ca.pem;
                        ssl_verify_client on;
                        ssl_protocols TLSv1.2 TLSv1.3;
                        proxy_pass host.testcontainers.internal:%d;
                      }
                    }
                    """.formatted(tcpProxyPort));
        }
        GenericContainer<?> nginx = new GenericContainer<>("nginx:1.27-alpine")
                .withExposedPorts(WSS_PORT, MTLS_PORT)
                .withAccessToHost(true)
                .withCopyToContainer(Transferable.of(conf.toString()), "/etc/nginx/nginx.conf")
                .withCopyToContainer(Transferable.of(pki.serverChainPem()), "/etc/nginx/certs/server.pem")
                .withCopyToContainer(Transferable.of(pki.serverKeyPem()), "/etc/nginx/certs/server-key.pem")
                .withCopyToContainer(Transferable.of(pki.caPem()), "/etc/nginx/certs/ca.pem")
                .withCopyToContainer(Transferable.of(user + ":{PLAIN}" + password + "\n"), "/etc/nginx/htpasswd")
                .waitingFor(Wait.forListeningPorts(wsProxyPort > 0 ? WSS_PORT : MTLS_PORT).withStartupTimeout(Duration.ofMinutes(2)));
        nginx.start();
        return nginx;
    }
}
