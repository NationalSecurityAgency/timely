package timely.model;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

/**
 * A simple interface that objects can implement to return the object size.
 */
public interface ObjectSizeOf {

    /**
     * The (approximate) size of the object
     */
    long sizeInBytes();

    public static class ObjectInstance {

        private Object o;
        private String name = "";

        public ObjectInstance(Object _o) {
            this.o = _o;
        }

        public ObjectInstance(Object _o, String name) {
            this.o = _o;
            this.name = name;
        }

        public Object getObject() {
            return o;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(o);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ObjectInstance)) {
                return false;
            }
            return ((ObjectInstance) obj).o == this.o;
        }
    }

    public static class Sizer {

        private static final Logger log = Logger.getLogger(Sizer.class.getCanonicalName());
        public static final short OBJECT_OVERHEAD = 8;
        public static final short ARRAY_OVERHEAD = 12;
        public static final short REFERENCE = 4;
        // The size of the basic Number constructs (and Boolean and Character)
        // is 16: roundUp(8 + primitiveSize)
        public static final short NUMBER_SIZE = 16;

        /**
         * Get the size of an object. Note that we want something relatively fast that
         * gives us an order of magnitude here. The java Instrumentation agent mechanism
         * is a little too costly for general use here. This will look for the
         * ObjectSizeOf interface and if implemented on the object will use that.
         * Otherwise it will do a simple navigation of the fields using reflection.
         *
         * @return an approximation of the object size
         */

        public static long getObjectSize(Object o) {
            return getObjectSize(o, true, false);
        }

        public static long getObjectSize(Object o, boolean useSizeInBytesMethod, boolean debug) {
            List<String> output = null;
            if (debug) {
                output = new ArrayList<>();
            }
            long size = getObjectSize(new ObjectInstance(o, o.getClass().getSimpleName()), new HashSet<>(), 0,
                    useSizeInBytesMethod, output);
            if (debug) {
                Collections.reverse(output);
                for (String s : output)
                    System.out.println(s);
            }
            return size;
        }

        public static long getObjectSize(ObjectInstance oi, Set<ObjectInstance> visited, int indent,
                boolean useSizeInBytesMethod, List<String> output) {
            long size = 0;
            Object o = oi.getObject();
            int arrayOverhead = 0;
            int objectOverhead = 0;
            int reference = 0;
            boolean roundUp = false;
            if (o != null && !visited.contains(oi)) {
                visited.add(oi);
                try {
                    if (useSizeInBytesMethod) {
                        if (o instanceof ObjectSizeOf) {
                            size = ((ObjectSizeOf) o).sizeInBytes();
                            if (output != null && size > 0) {
                                output.add(StringUtils.repeat(' ', indent) + oi.getName() + " [" + size + "] ("
                                        + o.getClass().getCanonicalName() + ") [ObjectSizeOf]");
                            }
                        }
                    }
                    if (size == 0) {
                        // the hard way...
                        // do not include Class related objects or
                        // reflection objects
                        if ((o instanceof Class) || (o instanceof ClassLoader) || (o.getClass().getPackage() != null
                                && o.getClass().getPackage().getName().startsWith("java.lang.reflect"))) {
                            size = 0;
                        } else if (o instanceof Number || o instanceof Boolean || o instanceof Character) {
                            size = NUMBER_SIZE;
                        } else {
                            // lets do a simple sizing
                            Class<?> c = o.getClass();
                            if (c.isArray()) {
                                arrayOverhead++;
                                int length = Array.getLength(o);
                                if (c.getComponentType().isPrimitive()) {
                                    size += length * getPrimitiveObjectSize(c.getComponentType());
                                } else {
                                    reference += length;
                                    for (int i = 0, index = length - 1; i < length; i++, index--) {
                                        Object element = Array.get(o, index);
                                        if (element != null) {
                                            size += getObjectSize(
                                                    new ObjectInstance(element, oi.getName() + "[" + index + "]"),
                                                    visited, indent + 2, useSizeInBytesMethod, output);
                                        }
                                    }
                                }
                            } else {
                                objectOverhead++;
                                while (c != null) {
                                    for (Field field : c.getDeclaredFields()) {
                                        if (Modifier.isStatic(field.getModifiers())) {
                                            continue;
                                        }
                                        if (field.getType().isPrimitive()) {
                                            long primSize = getPrimitiveObjectSize(field.getType());
                                            if (output != null) {
                                                output.add(StringUtils.repeat(' ', indent + 2) + field.getName() + " ["
                                                        + primSize + "] (" + field.getType().getCanonicalName() + ")");
                                            }
                                            size += primSize;
                                        } else {
                                            reference++;
                                            boolean accessible = field.isAccessible();
                                            field.setAccessible(true);
                                            try {
                                                Object fieldObject = field.get(o);
                                                if (fieldObject != null) {
                                                    size += getObjectSize(
                                                            new ObjectInstance(fieldObject, field.getName()), visited,
                                                            indent + 2, useSizeInBytesMethod, output);
                                                }
                                            } catch (Exception e) {
                                                // cannot get to field, so
                                                // ignore it in this size
                                                // calculation
                                                e.printStackTrace();
                                            }
                                            field.setAccessible(accessible);
                                        }
                                    }
                                    c = c.getSuperclass();
                                }
                            }
                            roundUp = true;
                        }
                    }
                    long underlyingObjects = size;
                    long overheads = REFERENCE * reference + OBJECT_OVERHEAD * objectOverhead
                            + ARRAY_OVERHEAD * arrayOverhead;
                    long addedOverheads = size + overheads;
                    if (roundUp) {
                        size = roundUp(addedOverheads);
                    } else {
                        size = addedOverheads;
                    }
                    if (output != null) {
                        output.add(StringUtils.repeat(' ', indent) + oi.getName() + " [" + size + "] ("
                                + o.getClass().getCanonicalName() + ") = " + underlyingObjects + "+" + overheads + "+"
                                + (size - addedOverheads) + " REF=" + reference + " OBJ=" + objectOverhead + " ARRAY="
                                + arrayOverhead + " ROUND=" + (size - addedOverheads));
                    }
                } catch (Throwable t) {
                    log.warning("Unable to determine object size for " + o);
                }
            }
            return size;
        }

        public static long roundUp(long size) {
            long extra = size % 8;
            if (extra > 0) {
                size = size + 8 - extra;
            }
            return size;
        }

        public static short getPrimitiveObjectSize(Class<?> primitiveType) {
            if (primitiveType.equals(int.class) || primitiveType.equals(float.class)) {
                return 4;
            } else if (primitiveType.equals(boolean.class) || primitiveType.equals(byte.class)) {
                return 1;
            } else if (primitiveType.equals(char.class) || primitiveType.equals(short.class)) {
                return 2;
            } else if (primitiveType.equals(long.class) || primitiveType.equals(double.class)) {
                return 8;
            } else { // if (primitiveType.equals(void.class)) {
                return 0;
            }
        }

        public static long getStringSize(String s) {
            long size = 0;
            size += ObjectSizeOf.Sizer.OBJECT_OVERHEAD;
            size += ObjectSizeOf.Sizer.REFERENCE;
            size += ObjectSizeOf.Sizer.getPrimitiveObjectSize(int.class);
            try {
                // change with Java 9
                if (String.class.getDeclaredField("coder") != null) {
                    size += ObjectSizeOf.Sizer.getPrimitiveObjectSize(byte.class);
                }
            } catch (Exception e) {

            }
            size += roundUp(ARRAY_OVERHEAD + (s.length() * ObjectSizeOf.Sizer.getPrimitiveObjectSize(byte.class)));
            return roundUp(size);
        }
    }

    public static void main(String[] args) {

        Metric m = new Metric("mymetric", 100000l, 32);
        m.addTag(new Tag("key1", "value1"));
        m.addTag(new Tag("key2", "value2"));
        m.addTag(new Tag("key3", "value3"));
        m.addTag(new Tag("key4", "value4"));
        m.addTag(new Tag("key5", "value5"));

        long newSize1 = ObjectSizeOf.Sizer.getObjectSize(m, false, true);
        System.out.println("new size #1: " + newSize1);

        System.out.println();
        System.out.println();
        System.out.println();
        long newSize2 = ObjectSizeOf.Sizer.getObjectSize(m, true, true);
        System.out.println("new size #2: " + newSize2);
    }

}
