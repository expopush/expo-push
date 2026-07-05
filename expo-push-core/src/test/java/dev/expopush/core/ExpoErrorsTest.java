package dev.expopush.core;

import dev.expopush.api.NotificationOutcome;
import dev.expopush.core.api.model.PushReceipt;
import dev.expopush.core.api.model.PushReceiptDetails;
import dev.expopush.core.api.model.PushTicket;
import dev.expopush.core.api.model.PushTicketDetails;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExpoErrorsTest {

    @Test
    void ticketErrorPrefersMachineReadableDetails() {
        PushTicket ticket = new PushTicket();
        PushTicketDetails details = new PushTicketDetails();
        details.setError("DeviceNotRegistered");
        ticket.setDetails(details);
        ticket.setMessage("human text");

        assertThat(ExpoErrors.errorOf(ticket)).isEqualTo("DeviceNotRegistered");
    }

    @Test
    void ticketErrorFallsBackToMessageThenPlaceholder() {
        PushTicket withMessage = new PushTicket();
        withMessage.setMessage("some error");
        assertThat(ExpoErrors.errorOf(withMessage)).isEqualTo("some error");

        assertThat(ExpoErrors.errorOf(new PushTicket())).isEqualTo("unknown ticket error");
        assertThat(ExpoErrors.errorOf((PushTicket) null)).isEqualTo("null ticket from Expo");
    }

    @Test
    void receiptErrorPrefersMachineReadableDetails() {
        PushReceipt receipt = new PushReceipt();
        PushReceiptDetails details = new PushReceiptDetails();
        details.setError("MessageRateExceeded");
        receipt.setDetails(details);
        receipt.setMessage("human text");

        assertThat(ExpoErrors.errorOf(receipt)).isEqualTo("MessageRateExceeded");
    }

    @Test
    void receiptErrorFallsBackToMessageThenPlaceholder() {
        PushReceipt withMessage = new PushReceipt();
        withMessage.setMessage("some error");
        assertThat(ExpoErrors.errorOf(withMessage)).isEqualTo("some error");

        assertThat(ExpoErrors.errorOf(new PushReceipt())).isEqualTo("unknown receipt error");
    }

    @Test
    void deviceNotRegisteredMapsToRejectedEverythingElseInvalid() {
        assertThat(ExpoErrors.outcomeFor("DeviceNotRegistered")).isEqualTo(NotificationOutcome.REJECTED);
        assertThat(ExpoErrors.outcomeFor("MessageTooBig")).isEqualTo(NotificationOutcome.INVALID);
        assertThat(ExpoErrors.outcomeFor(null)).isEqualTo(NotificationOutcome.INVALID);
    }
}
