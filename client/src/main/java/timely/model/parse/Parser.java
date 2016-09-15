package timely.model.parse;

@FunctionalInterface
public interface Parser<T> {

    T parse(String t);
}
