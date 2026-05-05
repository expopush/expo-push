package dev.expopush.core;

import dev.expopush.core.api.PushApi;
import dev.expopush.core.api.model.*;
import io.github.resilience4j.retry.Retry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ExpoGatewayTest {

    @Mock
    private PushApi pushApi;

    private ExpoGateway expoGateway;

    @BeforeEach
    void setUp() {
        Retry retry = Retry.ofDefaults("test");
        expoGateway = new ExpoGateway(pushApi, retry, retry);
    }

    @Test
    void sendNotificationsReturnsResponse() {
        PushMessage message = new PushMessage();
        message.setTo(Collections.singletonList("token"));
        List<PushMessage> messages = Collections.singletonList(message);
        
        PushTicketResponse expectedResponse = new PushTicketResponse();
        when(pushApi.sendNotifications(messages)).thenReturn(ResponseEntity.ok(expectedResponse));

        PushTicketResponse actualResponse = expoGateway.sendNotifications(messages);

        assertThat(actualResponse).isSameAs(expectedResponse);
        verify(pushApi).sendNotifications(messages);
    }

    @Test
    void getReceiptsReturnsResponse() {
        List<String> ticketIds = Collections.singletonList("ticket-id");
        PushReceiptResponse expectedResponse = new PushReceiptResponse();
        
        when(pushApi.getReceipts(any(PushReceiptRequest.class)))
                .thenReturn(ResponseEntity.ok(expectedResponse));

        PushReceiptResponse actualResponse = expoGateway.getReceipts(ticketIds);

        assertThat(actualResponse).isSameAs(expectedResponse);
        verify(pushApi).getReceipts(argThat(request -> 
                request.getIds().equals(ticketIds)));
    }
}
