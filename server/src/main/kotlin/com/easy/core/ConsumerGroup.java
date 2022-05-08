package com.easy.core;

import java.util.List;
import java.util.Objects;

public class ConsumerGroup {
    String groupName;
    List<Consumer> consumers;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerGroup that = (ConsumerGroup) o;
        return Objects.equals(groupName, that.groupName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupName);
    }

    /**
     * 获取下一个消费者
     * @return
     */
    public Consumer nextConsumer(){
        return null;
    }
}
