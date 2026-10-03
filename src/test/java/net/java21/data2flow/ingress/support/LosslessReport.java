package net.java21.data2flow.ingress.support;

import net.java21.data2flow.contracts.message.RawEnvelope;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * 무손실 시험 결과(reliability-and-ha.md §7 "유실과 중복은 보낸 메시지 ID 목록과 저장된 행을 비교"). 기준은 payload의 {@code seq}.
 *
 * <ul>
 *   <li>유실: 보낸 seq 중 스트림에 한 번도 없는 수</li>
 *   <li>중복(중복 제거 후): pipeline이 dedupKey로 거른 뒤에도 같은 seq가 2건 이상 남는 수 = seq 하나에 서로 다른 dedupKey가 2개 이상</li>
 *   <li>원본 중복: 스트림 기록 수 − 고유 seq 수(이중 수신 ADR-015·재전송으로 생긴 것, pipeline이 거른다)</li>
 * </ul>
 */
public record LosslessReport(String name, int sent, int rawRecords, int distinctSeq, int lost, int dupAfterDedup,
                             int rawDuplicates, Map<String, Integer> perInstance, Set<Integer> missing) {

    public static LosslessReport of(String name, int sent, List<RawEnvelope> envelopes, LoadGenerator load) {
        Map<Integer, Set<String>> keysBySeq = new HashMap<>();
        Map<String, Integer> perInstance = new HashMap<>();
        int raw = 0;
        for (RawEnvelope e : envelopes) {
            int seq = load.seqOf(e.payload());
            if (seq < 0) {
                continue;
            }
            raw++;
            keysBySeq.computeIfAbsent(seq, s -> new HashSet<>()).add(e.dedupKey());
            perInstance.merge(e.ingressInstance(), 1, Integer::sum);
        }
        Set<Integer> missing = new TreeSet<>();
        for (int i = 0; i < sent; i++) {
            if (!keysBySeq.containsKey(i)) {
                missing.add(i);
            }
        }
        int dup = (int) keysBySeq.values().stream().filter(k -> k.size() > 1).count();
        return new LosslessReport(name, sent, raw, keysBySeq.size(), missing.size(), dup, raw - keysBySeq.size(),
                perInstance, missing);
    }

    /** target/lossless-report.txt에 한 줄 남긴다(최종 보고용) */
    public LosslessReport write(String extra) {
        String line = name + ": sent=" + sent + ", rawRecords=" + rawRecords + ", distinctSeq=" + distinctSeq
                + ", lost=" + lost + ", dupAfterDedup=" + dupAfterDedup + ", rawDuplicates=" + rawDuplicates
                + ", perInstance=" + perInstance + (extra == null ? "" : ", " + extra) + System.lineSeparator();
        System.out.println("[LOSSLESS] " + line);
        try {
            Files.writeString(Path.of("target", "lossless-report.txt"), line, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return this;
    }
}
