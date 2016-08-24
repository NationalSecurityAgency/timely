package timely.validator;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.lang.reflect.InvocationTargetException;

/**
 * Implementation of {@link NotEmptyIfFieldSet} validator.
 */
public class NotEmptyIfFieldSetValidator implements ConstraintValidator<NotEmptyIfFieldSet, Object> {

    private String fieldName;
    private String fieldSetValue;
    private String notNullFieldName;

    @Override
    public void initialize(NotEmptyIfFieldSet annotation) {
        fieldName = annotation.fieldName();
        fieldSetValue = annotation.fieldValue();
        notNullFieldName = annotation.notNullFieldName();
    }

    @Override
    public boolean isValid(Object value, ConstraintValidatorContext context) {
        if (value == null) {
            return true;
        }

        try {
            final String fieldValue = BeanUtils.getProperty(value, fieldName);
            final String notNullFieldValue = BeanUtils.getProperty(value, notNullFieldName);
            if (StringUtils.equals(fieldValue, fieldSetValue) && StringUtils.isEmpty(notNullFieldValue)) {
                context.disableDefaultConstraintViolation();
                context.buildConstraintViolationWithTemplate(context.getDefaultConstraintMessageTemplate())
                        .addPropertyNode(notNullFieldName).addConstraintViolation();
                return false;
            }
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        return true;
    }
}
