package kafka.storm;

import java.io.Serializable;

public class HostPort implements Serializable {
	private static final long serialVersionUID = 4839058619688847826L;
	public String host;
    public int port;
    
    public HostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public HostPort(String host) {
        this(host, 9092);
    }

    @Override
    public boolean equals(Object o) {
        HostPort other = (HostPort) o;
        return host.equals(other.host) && port == other.port;
    }

    @Override
    public int hashCode() {
        return host.hashCode();
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
    
    
}
