package timely.api.request;

public interface UdpRequest {

    /**
     * Parse a line of text received and populate this object
     */
    public void parse(String line);

}
