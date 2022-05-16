Feature: Process company links information for happy path scenarios.

  Scenario: Consume the message and creating the company links successfully

    Given Company links consumer api service is running
    When a message is published for companyNumber "00006401" to update links
    Then the Company Links Consumer should send a PATCH request to the Company Profile API

  Scenario: Consume the message and update the company links successfully

    Given Company links consumer api service is running
    When a message is published for companyNumber "00006400" to check for links with status code "200"
    Then the Company Links Consumer should send a GET request to the Company Profile API

  Scenario: Consume the message and delete the company links successfully

    Given Company links consumer api service is running
    And company insolvency links exist for companyNumber "00006401"
    When a delete event is sent to kafka topic stream insolvency
    Then verify the company link is removed from company profile

  Scenario: Consume the message and should not invoke patch endpoint to delete the links

    Given Company links consumer api service is running
    When a delete event is sent to kafka topic stream insolvency companyNumber "00006401" which has no links
    Then verify the patch endpoint is never invoked to delete company links


