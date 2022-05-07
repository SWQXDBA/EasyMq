package entity;

import java.io.Serializable;

public class Entity implements Serializable {
    @Override
    public String toString() {
        return "Entity{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

    public Entity() {
    }

    public Entity(String name, int age) {
        this.name = name;
        this.age = age;
    }

    String name;
    int age;
}
