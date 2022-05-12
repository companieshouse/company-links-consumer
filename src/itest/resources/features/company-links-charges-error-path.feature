Feature: Process company links information for charges error scenarios

  Scenario Outline: Consume invalid charges message

    Given Company links consumer api service is running
    When a non-avro message is published to "stream-company-charges" topic and failed to process
    Then the message should be moved to topic "<topicName>" after retry attempts of "<retryAttempts>"

    Examples:
      | topicName                                                | retryAttempts |
      | stream-company-charges-company-links-consumer-invalid    | 0             |

  Scenario Outline: Consume valid charges avro message with invalid json

    Given Company links consumer api service is running
    When a valid message is published to "stream-company-charges" topic with invalid json
    Then the message should be moved to topic "<topicName>" after retry attempts of "<retryAttempts>"

    Examples:
      | topicName                                                | retryAttempts |
      | stream-company-charges-company-links-consumer-invalid    | 0             |


  Scenario Outline: Handle 4xx,5xx error from downstream call

    Given Company links consumer api service is running
    When a message is published to "stream-company-charges" topic for companyNumber "<companyNumber>" to check for links with status code "<statusCode>"
    Then the message should be moved to topic "<topicName>" after retry attempts of "<retryAttempts>"

    Examples:
      | companyNumber | statusCode | topicName                                             | retryAttempts |
      | 00006400      | 400        | stream-company-charges-company-links-consumer-invalid | 0             |
      | 00006400      | 503        | stream-company-charges-company-links-consumer-error   | 4             |
      | 00006400      | 404        | stream-company-charges-company-links-consumer-error   | 4             |
