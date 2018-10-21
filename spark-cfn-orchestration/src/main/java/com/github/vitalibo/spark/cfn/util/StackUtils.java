package com.github.vitalibo.spark.cfn.util;

import com.github.vitalibo.spark.emr.model.Batch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StackUtils {

    private static final Pattern PHYSICAL_RESOURCE_ID_PATTERN =
        Pattern.compile("(?<applicationId>application_[0-9]+_[0-9]+)#(?<batchId>[0-9]+)");

    public static String createPhysicalResourceId(Batch batch) {
        return String.format("%s#%s", batch.getApplicationId(), batch.getId());
    }

    public static Batch batchFromPhysicalResourceId(String physicalResourceId) {
        Matcher matcher = PHYSICAL_RESOURCE_ID_PATTERN.matcher(physicalResourceId);

        if (!matcher.matches()) {
            throw new IllegalStateException("'PhysicalResourceId' not match to pattern.");
        }

        return new Batch()
            .withApplicationId(matcher.group("applicationId"))
            .withBatchId(Integer.parseInt(matcher.group("batchId")));
    }

}