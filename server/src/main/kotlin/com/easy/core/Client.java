package com.easy.core;

public abstract class Client {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Client)) return false;

        Client client = (Client) o;

        if (!name.equals(client.name)) return false;
        if (!ip.equals(client.ip)) return false;
        return port.equals(client.port);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + ip.hashCode();
        result = 31 * result + port.hashCode();
        return result;
    }

    String name;
    String ip;
    String port;
}
