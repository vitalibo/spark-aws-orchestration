package com.github.vitalibo.spark.etl.util;

import com.github.vitalibo.spark.etl.model.ActivityException;
import com.github.vitalibo.spark.etl.model.ActivityInput;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;

public class Rules<Input extends ActivityInput> {

    private final Collection<Rule<Input>> rules;

    @SafeVarargs
    public Rules(Rule<Input>... rules) {
        this.rules = Arrays.asList(rules);
    }

    public void verify(Input input) throws ActivityException {
        rules.forEach(rule -> rule.accept(input));
    }

    @FunctionalInterface
    public interface Rule<T extends ActivityInput> extends Consumer<T> {
    }
}