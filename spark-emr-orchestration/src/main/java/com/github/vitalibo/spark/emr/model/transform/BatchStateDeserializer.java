package com.github.vitalibo.spark.emr.model.transform;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.github.vitalibo.spark.emr.model.BatchState;

import java.io.IOException;
import java.util.Objects;

public class BatchStateDeserializer extends JsonDeserializer<BatchState> {

    @Override
    public BatchState deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        String text = parser.getText();
        if (Objects.isNull(text)) {
            return null;
        }

        return BatchState.valueOf(text.toUpperCase());
    }

}
