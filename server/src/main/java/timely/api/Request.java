package timely.api;

public interface Request {

    /**
     * Validates the request, throws IllegalArgumentException if not valid.
     */
    default void validate() {

    }
}
