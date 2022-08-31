package timely.api.request;

public interface HttpPostRequest extends HttpRequest {

    HttpPostRequest parseBody(String content) throws Exception;

}
