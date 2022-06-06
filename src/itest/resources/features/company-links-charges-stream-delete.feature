Feature: Process company links charges stream delete scenarios


  Scenario Outline: Company profile exists with charges link but get charges returns empty list a kafka delete will patch the profile to remove the charges link

    Given Company links consumer api service is running
    And Company profile stubbed with charges links for "<companyNumber>"
    When A valid avro delete message for company number "<companyNumber>" is sent to the Kafka topic "<topicName>"
    Then The message is successfully consumed and company-profile-api PATCH endpoint is invoked removing charges link

    Examples:
      | companyNumber | topicName              |
      | 00006400      | stream-company-charges |
