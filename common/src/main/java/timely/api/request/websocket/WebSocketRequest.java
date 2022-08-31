package timely.api.request.websocket;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import timely.api.request.Request;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "operation")
public interface WebSocketRequest extends Request {}
