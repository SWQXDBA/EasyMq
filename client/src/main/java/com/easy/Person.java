package com.easy;

import java.time.LocalDateTime;

public class Person {
  public  int age = 5;

  public LocalDateTime localDateTime = LocalDateTime.now();

  @Override
  public String toString() {
    return "Person{" +
            "age=" + age +
            ", localDateTime=" + localDateTime +
            '}';
  }
}
