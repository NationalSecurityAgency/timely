package timely.api.request;

public interface HttpPostRequest extends Request {

    public HttpPostRequest parseBody(String content) throws Exception;

}
