package com.example.coffee;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

class KafkaOrderEventSenderTest {
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, OrderEventSender.OrderEvent> kafka = mock(KafkaTemplate.class);
    private final KafkaOrderEventSender sender = new KafkaOrderEventSender(kafka, "orders.paid");
    private final OrderEventSender.OrderEvent event = OrderEventSender.OrderEvent.builder()
            .eventId(7).orderId(8).userId(9).menuId(10).paidAmount(4500).build();

    @Test
    void publishesUserMenuAndAmountAfterBrokerAcknowledgement() {
        when(kafka.send("orders.paid", "9", event)).thenReturn(CompletableFuture.completedFuture(null));

        sender.send(event);

        verify(kafka).send("orders.paid", "9", event);
    }

    @Test
    void failedAcknowledgementKeepsOutboxEligibleForRetry() {
        when(kafka.send("orders.paid", "9", event))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));

        assertThatThrownBy(() -> sender.send(event))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Kafka publication failed");
    }
}
