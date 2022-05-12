Feature: Process company links information for error scenarios

  Scenario Outline: Consume invalid message

    Given Company links consumer api service is running
    When a non-avro message is published and failed to process
    Then the message should be moved to topic "<topicName>" after retry attempts of "<retryAttempts>"

    Examples:
      | topicName                                                | retryAttempts |
      | stream-company-insolvency-company-links-consumer-invalid | 0             |

  Scenario Outline: Consume valid avro message with invalid json

    Given Company links consumer api service is running
    When a valid message is published with invalid json
    Then the message should be moved to topic "<topicName>" after retry attempts of "<retryAttempts>"

    Examples:
      | topicName                                                | retryAttempts |
      | stream-company-insolvency-company-links-consumer-invalid | 0             |

  Scenario Outline: Handle 4xx,5xx error from downstream call

    Given Company links consumer api service is running
    When a message is published for companyNumber "<companyNumber>" to check for links with status code "<statusCode>"
    Then the message should be moved to topic "<topicName>" after retry attempts of "<retryAttempts>"

    Examples:
      | companyNumber | statusCode | topicName                                                | retryAttempts |
      | 00006400      | 400        | stream-company-insolvency-company-links-consumer-invalid | 0             |
      | 00006400      | 503        | stream-company-insolvency-company-links-consumer-error   | 4             |
      | 00006400      | 404        | stream-company-insolvency-company-links-consumer-error   | 4             |


  Scenario Outline: Handle 2xx error from downstream call with the response throwing NPE

    Given Company links consumer api service is running
    When a message is published for companyNumber "<companyNumber>" to update links with a null attribute
    Then the message should be moved to topic "<topicName>" after retry attempts of "<retryAttempts>"
    Examples:
      | companyNumber | topicName                                              | retryAttempts |
      | 00006401      | stream-company-insolvency-company-links-consumer-error | 4             |


