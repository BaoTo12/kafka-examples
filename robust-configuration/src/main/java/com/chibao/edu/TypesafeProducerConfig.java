package com.chibao.edu;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serializer;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;

public final class TypesafeProducerConfig {
    public static final class UnsupportedPropertyException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private UnsupportedPropertyException(String s) {
            super(s);
        }
    }

    public static final class ConflictingPropertyException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private ConflictingPropertyException(String s) {
            super(s);
        }
    }

    private String bootstrapServers;

    private Class<? extends Serializer<?>> keySerializerClass;

    private Class<? extends Serializer<?>> valueSerializerClass;

    private final Map<String, Object> customEntries = new HashMap<>();

    public TypesafeProducerConfig withBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public TypesafeProducerConfig withKeySerializerClass(Class<? extends Serializer<?>> keySerializerClass) {
        this.keySerializerClass = keySerializerClass;
        return this;
    }

    public TypesafeProducerConfig withValueSerializerClass(Class<? extends Serializer<?>> valueSerializerClass) {
        this.valueSerializerClass = valueSerializerClass;
        return this;
    }

    public TypesafeProducerConfig withCustomEntry(String propertyName, Object value) {
        Objects.requireNonNull(propertyName, "Property name cannot be null");
        customEntries.put(propertyName, value);
        return this;
    }

    // ! Kiểm tra custom properties có hợp lệ không
    public Map<String, Object> mapify() {
        final var stagingConfig = new HashMap<String, Object>();
        if (!customEntries.isEmpty()) {
            // ! lấy danh sách tất cả property name hợp lệ trong:
            //ProducerConfig
            //SaslConfigs
            //SecurityConfig
            // ! Dùng reflection quét qua các hằng số (static final String) trong các class đó.
            // ? Nếu user nhập sai tên property --> mapify() sẽ ném UnsupportedPropertyException.
            // ?
            final var supportedKeys = scanClassesForPropertyNames(SecurityConfig.class,
                    SslConfigs.class,
                    SaslConfigs.class,
                    ProducerConfig.class,
                    CommonClientConfigs.class);
            final var unsupportedKey = customEntries.keySet()
                    .stream()
                    .filter(not(supportedKeys::contains))
                    .findAny();

            if (unsupportedKey.isPresent()) {
                throw new UnsupportedPropertyException("Unsupported property " + unsupportedKey.get());
            }

            stagingConfig.putAll(customEntries);
        }
        // ! Thêm các property bắt buộc
        Objects.requireNonNull(bootstrapServers, "Bootstrap servers not set");
        tryInsertEntry(stagingConfig, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        Objects.requireNonNull(keySerializerClass, "Key serializer not set");
        tryInsertEntry(stagingConfig, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());
        Objects.requireNonNull(valueSerializerClass, "Value serializer not set");
        tryInsertEntry(stagingConfig, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());

        return stagingConfig;
    }

    private static void tryInsertEntry(Map<String, Object> staging, String key, Object value) {
        staging.compute(key, (__key, existingValue) -> {
            if (existingValue == null) {
                return value;
            } else {
                throw new ConflictingPropertyException("Property " + key + " conflicts with an expected property");
            }
        });
    }

    // ! lấy danh sách tất cả property name hợp lệ trong:
    // * How it works
    /*
    * Bước 1 — Truyền vào danh sách các class: scanClassesForPropertyNames(ProducerConfig.class);
    * nghĩa là bạn muốn quét tất cả public static final String trong ProducerConfig.
    * Bước 2 — Arrays.stream(classes)
    * Chuyển mảng class đầu vào ([ProducerConfig.class]) thành Stream để xử lý tuần tự.
        Bước 3 — .map(Class::getFields)
        Class::getFields() trả về tất cả public field (biến static, constant, v.v.) của mỗi class.
        Ví dụ (giả sử trong ProducerConfig có):
           public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
        public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
        public static final String ACKS_CONFIG = "acks";
        public static final String BOOTSTRAP_SERVERS_DOC = "docs for bootstrap";
        * Khi gọi:

        ProducerConfig.class.getFields()
        * 👉 trả về Field[] gồm 4 phần tử (tương ứng 4 biến ở trên).

        Bước 4 — .flatMap(Arrays::stream)

        Nối toàn bộ Field[] từ tất cả class lại thành một luồng duy nhất (Stream<Field>).

        Giả sử bạn truyền vào 3 class (ProducerConfig, SaslConfigs, SecurityConfig),
        thì flatMap sẽ hợp tất cả các field của 3 class thành một stream duy nhất.

        Bước 5 — .filter(TypesafeProducerConfig::isFieldConstant)

        Giữ lại chỉ những field là hằng số (static final).

        Ví dụ:

        public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";  ✅
        public final String SOME_VAR = "abc";                                      ❌ (thiếu static)
        public static String OTHER = "def";                                        ❌ (thiếu final)


        Hàm isFieldConstant(Field f) kiểm tra:

        Modifier.isFinal(f.getModifiers()) && Modifier.isStatic(f.getModifiers());

        Bước 6 — .filter(TypesafeProducerConfig::isFieldStringType)

        Giữ lại chỉ những field có kiểu String
        (vì ta chỉ quan tâm đến tên property chứ không phải số hay kiểu khác).

        Ví dụ:

        public static final int DEFAULT_BUFFER_SIZE = 1024;   ❌ bị loại
        public static final String ACKS_CONFIG = "acks";      ✅ giữ lại

        Bước 7 — .filter(not(TypesafeProducerConfig::isFieldDoc))

        Bỏ qua các field kết thúc bằng _DOC (chứa mô tả, không phải key thật).

        Ví dụ:

        public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers"; ✅
        public static final String BOOTSTRAP_SERVERS_DOC = "..."                   ❌


        Hàm kiểm tra:

        field.getName().endsWith("_DOC")

        Bước 8 — .map(TypesafeProducerConfig::retrieveField)

        Lấy giá trị của field ra (thay vì đối tượng Field).
        Dùng reflection:

        field.get(null)


        vì field là static, không cần instance.
        Kết quả là "bootstrap.servers", "acks", "key.serializer", v.v.

        Bước 9 — .collect(Collectors.toSet())

        Thu thập tất cả tên property thành Set<String> (không trùng lặp).

        🧠 Kết quả thực tế (ví dụ demo)

        Ví dụ code chạy thử rút gọn:

        import org.apache.kafka.clients.producer.ProducerConfig;
        import java.util.*;

        public class Demo {
            public static void main(String[] args) {
                Set<String> props = scanClassesForPropertyNames(ProducerConfig.class);
                props.stream().limit(5).forEach(System.out::println);
            }

            private static Set<String> scanClassesForPropertyNames(Class<?>... classes) {
                return Arrays.stream(classes)
                    .map(Class::getFields)
                    .flatMap(Arrays::stream)
                    .filter(f -> Modifier.isFinal(f.getModifiers()) && Modifier.isStatic(f.getModifiers()))
                    .filter(f -> f.getType().equals(String.class))
                    .filter(f -> !f.getName().endsWith("_DOC"))
                    .map(f -> {
                        try { return (String) f.get(null); }
                        catch (Exception e) { throw new RuntimeException(e); }
                    })
                    .collect(Collectors.toSet());
            }
        }
        👉 Output (ví dụ)
        bootstrap.servers
        acks
        key.serializer
        value.serializer
        buffer.memory
    * *
    * **/
    private static Set<String> scanClassesForPropertyNames(Class<?>... classes) {
        return Arrays.stream(classes)
                .map(Class::getFields)
                .flatMap(Arrays::stream)
                .filter(TypesafeProducerConfig::isFieldConstant) // static final
                .filter(TypesafeProducerConfig::isFieldStringType) // kiểu String
                .filter(not(TypesafeProducerConfig::isFieldDoc)) // bỏ mấy field *_DOC
                .map(TypesafeProducerConfig::retrieveField) // lấy giá trị của hằng
                .collect(Collectors.toSet());
    }

    private static boolean isFieldConstant(Field field) {
        return Modifier.isFinal(field.getModifiers()) && Modifier.isStatic(field.getModifiers());
    }

    private static boolean isFieldStringType(Field field) {
        return field.getType().equals(String.class);
    }

    private static boolean isFieldDoc(Field field) {
        return field.getName().endsWith("_DOC");
    }

    private static String retrieveField(Field field) {
        try {
            return (String) field.get(null);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}