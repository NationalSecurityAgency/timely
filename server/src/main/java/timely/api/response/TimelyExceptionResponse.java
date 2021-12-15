package timely.api.response;

import java.io.Serializable;

public class TimelyExceptionResponse implements Serializable {

    private static final long serialVersionUID = 8951492383251242114L;

    public String message;
    public String detailMessage;
    public int responseCode;

    public TimelyExceptionResponse(TimelyException e) {
        this.message = e.getMessage();
        this.detailMessage = e.getDetails();
        this.responseCode = e.getCode();
    }
}
