package com.singlestore.fivetran.source.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

public class StateTest {

  @Test
  public void createFromJson() throws Exception {
    String json = "{\"offsets\":[\"a\",\"b\",null,\"csds\"]}";
    State s = State.fromJson(json);
    assertEquals("'a', 'b', NULL, 'csds'", s.offsetsAsSQL());
    assertEquals(json, s.toJson());
  }

  @Test
  public void createFromNumPartitions() throws JsonProcessingException {
    State s = new State(0);
    assertEquals("", s.offsetsAsSQL());
    assertEquals("{\"offsets\":[]}", s.toJson());

    s = new State(3);
    assertEquals("NULL, NULL, NULL", s.offsetsAsSQL());
    assertEquals("{\"offsets\":[null,null,null]}", s.toJson());
  }

  @Test
  public void setOffset() {
    State s = new State(3);
    assertEquals("NULL, NULL, NULL", s.offsetsAsSQL());

    s.setOffset(1, "a");
    assertEquals("NULL, 'a', NULL", s.offsetsAsSQL());

    s.setOffset(1, "b");
    assertEquals("NULL, 'b', NULL", s.offsetsAsSQL());

    s.setOffset(2, "cd");
    assertEquals("NULL, 'b', 'cd'", s.offsetsAsSQL());

    s.setOffset(0, "w");
    assertEquals("'w', 'b', 'cd'", s.offsetsAsSQL());
  }
}
