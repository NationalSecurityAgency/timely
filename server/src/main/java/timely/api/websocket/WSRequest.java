package timely.api.websocket;

import timely.api.AuthenticatedRequest;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "operation")
@JsonSubTypes({ @Type(name = "create", value = CreateSubscription.class),
        @Type(name = "add", value = AddSubscription.class), @Type(name = "remove", value = RemoveSubscription.class),
        @Type(name = "close", value = CloseSubscription.class) })
public class WSRequest extends AuthenticatedRequest {

}
