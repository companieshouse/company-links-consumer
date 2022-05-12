Feature: Process company links information for charges error scenarios

  Scenario Outline: Consume invalid message

    Given Company links consumer api service is running
    When a non-avro message is published to charges topic and failed to process
    Then the message should be moved to topic "<topicName>" after retry attempts of "<retryAttempts>"

    Examples:
      | topicName                                                | retryAttempts |
      | stream-company-charges-company-links-consumer-invalid    | 0             |

