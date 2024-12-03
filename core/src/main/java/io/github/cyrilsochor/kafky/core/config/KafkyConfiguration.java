package io.github.cyrilsochor.kafky.core.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;

import java.util.Map;

public record KafkyConfiguration(

        @JsonProperty("components") @JsonSetter(nulls = Nulls.AS_EMPTY) Map<Object, Object> components,
        @JsonProperty("consumers") @JsonSetter(nulls = Nulls.AS_EMPTY) Map<Object, Object> consumers,
        @JsonProperty("global-consumers") @JsonSetter(nulls = Nulls.AS_EMPTY) Map<Object, Object> globalConsumers,
        @JsonProperty("producers") @JsonSetter(nulls = Nulls.AS_EMPTY) Map<Object, Object> producers,
        @JsonProperty("global-producers") @JsonSetter(nulls = Nulls.AS_EMPTY) Map<Object, Object> globalProducers,
        @JsonProperty("global") @JsonSetter(nulls = Nulls.AS_EMPTY) Map<Object, Object> global,
        @JsonProperty("report") @JsonSetter(nulls = Nulls.AS_EMPTY) Map<Object, Object> report

) {

}
