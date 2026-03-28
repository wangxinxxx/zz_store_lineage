package com.zhuanzhuan.lineage.common;

import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public final class ScalaInterop {
    private ScalaInterop() {
    }

    public static <T> List<T> toJavaList(Seq<T> seq) {
        if (seq == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(JavaConverters.seqAsJavaListConverter(seq).asJava());
    }

    public static <T> Optional<T> toOptional(Option<T> option) {
        if (option == null || option.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(option.get());
    }
}
