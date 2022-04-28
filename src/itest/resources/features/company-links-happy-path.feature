Feature: Process company links information for happy path scenarios.

  Scenario: Consume the message and creating the company links successfully

    Given Company links consumer api service is running
    When a message is published for companyNumber "00006401" to update links
    Then the Company Links Consumer should send a PATCH request to the Company Profile API

  Scenario: Consume the message and update the company links successfully

    Given Company links consumer api service is running
    When a message is published for companyNumber "00006400" to check for links with status code "200"
    Then the Company Links Consumer should send a GET request to the Company Profile API


