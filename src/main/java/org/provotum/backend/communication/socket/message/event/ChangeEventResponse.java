package org.provotum.backend.communication.socket.message.event;

import org.provotum.backend.communication.message.base.AResponse;
import org.provotum.backend.communication.message.base.ResponseType;
import org.provotum.backend.communication.message.base.Status;

public class ChangeEventResponse extends AResponse {

    private String senderAddress;
    private final ResponseType responseType = ResponseType.CHANGE_EVENT;

    public ChangeEventResponse(String id, Status status, String message, String senderAddress) {
        super(id, status, message);
        this.senderAddress = senderAddress;
    }

    public ChangeEventResponse(Status status, String message, String senderAddress) {
        super(status, message);
        this.senderAddress = senderAddress;
    }

    public String getSenderAddress() {
        return senderAddress;
    }

    public ResponseType getResponseType() {
        return responseType;
    }
}
