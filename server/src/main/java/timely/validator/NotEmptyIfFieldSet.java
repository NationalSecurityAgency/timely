package timely.validator;

import javax.validation.Constraint;
import javax.validation.Payload;
import javax.validation.ReportAsSingleViolation;
import javax.validation.constraints.NotNull;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Validates that field {@code notNullFieldName} is not null if
 * {@code fieldName} is set to {@code fieldValue}
 */
@Documented
@Constraint(validatedBy = { NotEmptyIfFieldSetValidator.class })
@Target({ TYPE, ANNOTATION_TYPE })
@Retention(RUNTIME)
@ReportAsSingleViolation
@NotNull
public @interface NotEmptyIfFieldSet {

    String fieldName();

    String fieldValue();

    String notNullFieldName();

    String message() default "{NotEmptyIfFieldSet.message}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    @Target({ TYPE, ANNOTATION_TYPE })
    @Retention(RUNTIME)
    @Documented
    @interface List {

        NotEmptyIfFieldSet[] value();
    }
}
