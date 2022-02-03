package uk.gov.companieshouse.company.links.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.company.links.processor.ChargesStreamProcessor;
import uk.gov.companieshouse.delta.ChsDelta;

//TODO Do we need to load a component based on the property value as suggested by 'Dom' //Action: Zaid to confirm
@Component
public class ChargesStreamConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChargesStreamConsumer.class);

  private final ChargesStreamProcessor streamProcessor;

  @Autowired
  public ChargesStreamConsumer(ChargesStreamProcessor streamProcessor) {
    this.streamProcessor = streamProcessor;
  }

  //TODO is groupId is same as topicId.  // name of compoent will be groupId
  @KafkaListener(topics = "${charges.stream.topic.main}", groupId="charges.stream.topic.main")
  @Retryable
  public void receive(Message<ChsDelta> chsDeltaMessage) {
    LOGGER.info("A new message read from MAIN topic with payload: " + chsDeltaMessage.getPayload());
    streamProcessor.process(chsDeltaMessage);
  }

  @KafkaListener(topics = "${charges.stream.topic.retry}", groupId = "charges.stream.topic.retry")
  public void retry(Message<ChsDelta> chsDeltaMessage) {
    LOGGER.info(String.format("A new message read from RETRY topic with payload:%s and headers:%s ", chsDeltaMessage.getPayload(), chsDeltaMessage.getHeaders()));
    streamProcessor.process(chsDeltaMessage);
  }

  @KafkaListener(topics = "${charges.stream.topic.error}", groupId = "charges.stream.topic.error")
  public void error(Message<ChsDelta> chsDeltaMessage) {
    LOGGER.info(String.format("A new message read from ERROR topic with payload:%s and headers:%s ", chsDeltaMessage.getPayload(), chsDeltaMessage.getHeaders()));
    streamProcessor.process(chsDeltaMessage);
  }

}
