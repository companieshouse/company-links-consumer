Feature: Process company links information for error scenarios

  Scenario Outline: Company profile exists with zero charges

    Given Company links consumer api service is running
    And Company profile returns response "profile-with-out-charges" for company number "<companyNumber>"
    When A valid avro message is sent to the Kafka topic "<topicName>"
    Then The message is successfully consumed and company-profile-api PATCH endpoint is invoked with charges link payload

    Examples:
      | companyNumber | topicName              |
      | 00006400      | stream-company-charges |
      | 12345678      | stream-company-charges |
      | SC987654      | stream-company-charges |

  Scenario Outline: Company profile exists with charges

    Given Company links consumer api service is running
    And Company profile returns response "profile-with-charges-links" for company number "<companyNumber>"
    When A valid avro message is sent to the Kafka topic "<topicName>"
    Then The message is successfully consumed and company-profile-api PATCH endpoint is NOT invoked

    Examples:
      | companyNumber | topicName              |
      | 00006400      | stream-company-charges |
      | 13254876      | stream-company-charges |
      | SC987654      | stream-company-charges |

