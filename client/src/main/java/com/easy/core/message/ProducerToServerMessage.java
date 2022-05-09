package com.easy.core.message;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class ProducerToServerMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    public String producerName;
    public Set<ProducerToServerMessageUnit> messages = new HashSet<>();
}