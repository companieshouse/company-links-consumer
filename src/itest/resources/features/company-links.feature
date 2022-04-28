Feature: Process company links information

  Scenario: Consume the message and creating the company links successfully

    Given Company links consumer api service is running
    When a message is published for companyNumber "00006401" to update links
    Then the Company Links Consumer should send a PATCH request to the Company Profile API

  Scenario: Consume the message and update the company links successfully

    Given Company links consumer api service is running
    When a message is published for companyNumber "00006400" to check for links with status code "200"
    Then the Company Links Consumer should send a GET request to the Company Profile API

  Scenario: Consume invalid message

    Given Company links consumer api service is running
    When a non-avro message is published and failed to process
    Then the message should be moved to topic "stream-company-insolvency-company-links-consumer-invalid"

  Scenario: Consume valid avro message with invalid json

    Given Company links consumer api service is running
    When a valid message is published with invalid json
    Then the message should be moved to topic "stream-company-insolvency-company-links-consumer-invalid"

  Scenario Outline: Handle 4xx,5xx error from downstream call

    Given Company links consumer api service is running
    When a message is published for companyNumber "<companyNumber>" to check for links with status code "<statusCode>"
    Then the message should be moved to topic "<topicName>"

    Examples:
      | companyNumber | statusCode | topicName                                                |
      | 00006400      | 400        | stream-company-insolvency-company-links-consumer-invalid |
      | 00006400      | 503        | stream-company-insolvency-company-links-consumer-error   |
      | 00006400      | 404        | stream-company-insolvency-company-links-consumer-error   |

  Scenario: Handle 2xx error from downstream call with the response throwing NPE

    Given Company links consumer api service is running
    When a message is published for companyNumber "00006401" to update links with a null attribute
    Then the message should be moved to topic "stream-company-insolvency-company-links-consumer-error"


