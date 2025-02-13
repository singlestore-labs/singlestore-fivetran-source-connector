package com.singlestore.fivetran.source.connector;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class State {

  private static final ObjectMapper mapper = new ObjectMapper();

  @JsonProperty
  private List<String> offsets;

  static State fromJson(String json) throws JsonProcessingException {
    return mapper.readValue(json, State.class);
  }

  State(@JsonProperty("offsets") List<String> offsets) {
    this.offsets = offsets;
  }

  State(Integer numPartitions) {
    offsets = new ArrayList<>(Collections.nCopies(numPartitions, null));
  }

  public String toJson() throws JsonProcessingException {
    return mapper.writeValueAsString(this);
  }

  public String offsetsAsSQL() {
    return offsets
        .stream()
        .map(o -> o == null ? "NULL" : "'" + o + "'")
        .collect(Collectors.joining(", "));
  }

  public void setOffset(Integer index, String offset) {
    offsets.set(index, offset);
  }
}
