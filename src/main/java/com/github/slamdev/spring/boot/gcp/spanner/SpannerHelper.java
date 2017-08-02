package com.github.slamdev.spring.boot.gcp.spanner;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type.StructField;

import java.util.*;
import java.util.function.Function;

import static java.util.Optional.empty;
import static java.util.Optional.of;

public final class SpannerHelper {

    private SpannerHelper() {
        // Utility class
    }

    @SuppressWarnings({"LoopStatementThatDoesntLoop", "PMD.AvoidBranchingStatementAsLastInLoop"})
    public static <T> Optional<T> map(ResultSet resultSet, Function<ResultSet, T> rowMapper) {
        while (resultSet.next()) {
            return of(rowMapper.apply(resultSet));
        }
        return empty();
    }

    public static <T> Optional<T> map(Struct struct, Function<Struct, T> rowMapper) {
        return struct == null ? empty() : of(rowMapper.apply(struct));
    }

    public static <T> List<T> mapAll(ResultSet resultSet, Function<ResultSet, T> rowMapper) {
        List<T> results = new ArrayList<>();
        while (resultSet.next()) {
            results.add(rowMapper.apply(resultSet));
        }
        return results;
    }

    public static <T, K> Map<T, K> map(ResultSet resultSet, Function<ResultSet, T> keyMapper, Function<ResultSet, K> valueMapper) {
        Map<T, K> results = new HashMap<>();
        while (resultSet.next()) {
            results.put(keyMapper.apply(resultSet), valueMapper.apply(resultSet));
        }
        return results;
    }

    public static boolean isColumnExists(String name, ResultSet rs) {
        return rs.getType().getStructFields().stream().map(StructField::getName).anyMatch(name::equals);
    }

    public static void batchWrite(DatabaseClient db, List<Mutation> input, int limit) {
        List<Mutation> mutations = new ArrayList<>(input);
        while (mutations.size() >= limit) {
            List<Mutation> part = mutations.subList(0, limit);
            db.write(part);
            part.clear();
        }
        if (!mutations.isEmpty()) {
            db.write(mutations);
        }
    }
}
