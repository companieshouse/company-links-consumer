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

  Scenario: Non Avro message should fail to consume and message should be in to invalid topic
    Given Company Links Consumer component is successfully running
    And Stubbed Company profile API endpoint will return 200 http response code
    When A non-avro format random message is sent to the Kafka topic "stream-company-charges"
    Then The message is successfully consumed only once from the "stream-company-charges" topic
    But  Failed to process and immediately moved the message into "stream-company-charges-company-links-consumer-invalid" topic
    And Metrics Data API endpoint is never invoked

  Scenario: A valid avro message with invalid url format should fail to process and message sent to invalid topic
    Given Company Links Consumer component is successfully running
    And Stubbed Company profile API endpoint will return 200 http response code
    When  A valid message in avro format message with an invalid url format is sent in resource_uri attribute to the Kafka topic "stream-company-charges"
    Then  The message is successfully consumed only once from the "stream-company-charges" topic
    But   Failed to process and immediately moved the message into "stream-company-charges-company-links-consumer-invalid" topic
    And   Metrics Data API endpoint is never invoked

  Scenario: Should fail to process and message sent to invalid topic when company profile patch api returns 400 bad request
    Given Company Links Consumer component is successfully running
    And   Stubbed Company Profile API PATCH endpoint will return 400 bad request http response code
    When  A valid Avro message with valid json payload is sent to the Kafka topic "stream-company-charges" topic
    Then  The message is successfully consumed only once from the "stream-company-charges" topic
    But   Failed to process and immediately moved the message into "stream-company-charges-company-links-consumer-invalid" topic
    And  Metrics Data API endpoint is never invoked

  Scenario: Should fail to process and message sent to error topic when company profile GET endpoint is down
    Given Company Links Consumer component is successfully running
    And  Stubbed Company Profile API GET endpoint is down
    When A valid Avro message with valid json payload is sent to the Kafka topic "stream-company-charges" topic
    Then The message should be retried with 4 attempts and on retry exhaustion the message is finally sent into "stream-company-charges-company-links-consumer-error" topic

  Scenario: Should fail to process and message sent to error topic when company profile PATCH endpoint throws internal sever exception
    Given Company Links Consumer component is successfully running
    And   Stubbed Company Profile API PATCH endpoint throws an internal exception
    When  A valid Avro message with valid json payload is sent to the Kafka topic "stream-company-charges" topic
    Then  The message is successfully consumed only once from the "stream-company-charges" topic
    Then The message should be retried with 4 attempts and on retry exhaustion the message is finally sent into "stream-company-charges-company-links-consumer-error" topic

  Scenario: Consume the message for company links charges - Update Flow
    Given Company Links Consumer component is successfully running
    And calling the GET insolvency-data-api with companyNumber "00006400" returns status code "410" and insolvency is gone
    When  A valid Avro message with valid json payload is sent to the Kafka topic "stream-company-charges" topic
    When a message is published to the "stream-company-charges" topic with companyNumber "00006400" to update links
    Then The message should be retried with 4 attempts and on retry exhaustion the message is finally sent into "stream-company-charges-company-links-consumer-error" topic

  Scenario: Consume the message for company links charges - Delete Flow
    Given Company Links Consumer component is successfully running
    And calling the GET insolvency-data-api with companyNumber "00006400" returns status code "200"
    When A valid avro delete message for company number "00006400" is sent to the Kafka topic "stream-company-charges"
    Then The message should be retried with 4 attempts and on retry exhaustion the message is finally sent into "stream-company-charges-company-links-consumer-error" topic