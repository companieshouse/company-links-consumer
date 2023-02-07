package uk.gov.companieshouse.company.links.consumer;

import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import uk.gov.companieshouse.company.links.exception.NonRetryableErrorException;

@Aspect
public class KafkaMessageConsumerAspect {

    private final ResettableCountDownLatch resettableCountDownLatch;

    public KafkaMessageConsumerAspect(ResettableCountDownLatch resettableCountDownLatch) {
        this.resettableCountDownLatch = resettableCountDownLatch;
    }

    @AfterReturning(value = "execution(* uk.gov.companieshouse.company.links.consumer.InsolvencyStreamConsumer.receive(..)) " +
            "|| execution(* uk.gov.companieshouse.company.links.consumer.ChargesStreamConsumer.receive(..))" +
            "|| execution(* uk.gov.companieshouse.company.links.consumer.ExemptionsStreamConsumer.receive(..))")
    void onSuccessfulProcessing() {
        resettableCountDownLatch.countDownAll();
    }

    @AfterThrowing(value = "execution(* uk.gov.companieshouse.company.links.consumer.InsolvencyStreamConsumer.receive(..)) " +
            "|| execution(* uk.gov.companieshouse.company.links.consumer.ChargesStreamConsumer.receive(..))" +
            "|| execution(* uk.gov.companieshouse.company.links.consumer.ExemptionsStreamConsumer.receive(..))", throwing = "ex")
    void onConsumerException(Exception ex) {
        if (ex instanceof NonRetryableErrorException) {
            resettableCountDownLatch.countDownAll();
        } else {
            resettableCountDownLatch.countDown();
        }
    }

    @AfterThrowing(value = "execution(* org.apache.kafka.common.serialization.Deserializer.deserialize(..))", throwing = "ex")
    void deserialize() {
        resettableCountDownLatch.countDownAll();
    }
}
