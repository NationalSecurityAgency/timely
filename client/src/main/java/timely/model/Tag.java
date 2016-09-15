package timely.model;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import timely.model.parse.TagParser;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

/**
 * Tag consists of key value pair
 */
@XmlRootElement(name = "tag")
public class Tag implements Comparable<Tag>, Serializable {

    private static final Joiner equalJoiner = Joiner.on("=");
    private static final TagParser tagParser = new TagParser();

    private String key;
    private String value;

    public Tag() {
    }

    public Tag(Tag other) {
        this.setKey(other.getKey());
        this.setValue(other.getValue());
    }

    public Tag(String tag){
        this(tagParser.parse(tag));
    }
    public Tag(String key, String value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        this.setKey(key);
        this.setValue(value);
    }

    @XmlElement(name = "key")
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @XmlElement(name = "value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Joins key and value into key=value
     * 
     * @return
     */
    public String join() {
        return equalJoiner.join(key, value);
    }

    @Override
    public String toString() {
        return "Tag{" + equalJoiner.join(this.getKey(), this.getValue()) + "}";
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (null == o || getClass() != o.getClass())
            return false;

        return this.toString().equals(o.toString());
    }

    @Override
    public int compareTo(Tag other) {
        return this.toString().compareTo(other.toString());
    }
}
