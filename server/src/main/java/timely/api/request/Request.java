package timely.api.request;

/**
 * Marker interface for a request
 */
public interface Request {

    /**
     * Validates the request, throws IllegalArgumentException if not valid.
     */
    default void validate() {

    }
}
