package timely.model.parse;

public interface Combiner<T> {

    String combine(T o);
}
