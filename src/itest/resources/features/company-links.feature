Feature: Process company links information

  Scenario: Consume the message and update the company links

    Given Company links consumer api service is running
    When a message is published to the topic "stream-insolvency" for companyNumber "00006400"
    Then the Company Links Consumer should send a GET request to the Company Profile API

  Scenario: Consume the message and creating the company links

    Given Company links consumer api service is running
    When a message is published to the topic "stream-insolvency" for companyNumber "00006401"
    Then the Company Links Consumer should send a PATCH request to the Company Profile API