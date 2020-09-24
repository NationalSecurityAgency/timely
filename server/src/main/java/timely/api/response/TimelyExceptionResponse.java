package timely.api.response;

import java.io.Serializable;

public class TimelyExceptionResponse implements Serializable {

    public String message;
    public String detailMessage;
    public int responseCode;

    public TimelyExceptionResponse(TimelyException e) {
        this.message = e.getMessage();
        this.detailMessage = e.getDetails();
        this.responseCode = e.getCode();
    }
}
