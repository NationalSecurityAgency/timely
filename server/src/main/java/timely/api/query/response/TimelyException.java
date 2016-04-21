package timely.api.query.response;

public class TimelyException extends Exception {

    private static final long serialVersionUID = 1L;
    private int code = 500;
    private String details = null;

    public TimelyException(int code, String message, String details) {
        this(code, message, details, null);
    }

    public TimelyException(int code, String message, String details, Throwable error) {
        super(message, error);
        this.code = code;
        this.details = details;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }
}
