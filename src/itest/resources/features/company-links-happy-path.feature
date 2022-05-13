Feature: Process company links information for happy path scenarios.

  Scenario Outline: Consume the message and creating the company links successfully

    Given Company links consumer api service is running
    And Company profile returns response "profile-with-null-attribute" for company number "<companyNumber>"
    When a message is published to "<topicName>" topic for companyNumber "<companyNumber>" to update links
    Then the Company Links Consumer should send a PATCH request to the Company Profile API

    Examples:
      | companyNumber | topicName              |
      | 00006401      | stream-company-insolvency |


  Scenario Outline: Consume the message and update the company links successfully

    Given Company links consumer api service is running
    And Company profile returns response "profile-with-charges-links" for company number "<companyNumber>"
    When a message is published to "<topicName>" topic for companyNumber "00006400" to update links
    Then the Company Links Consumer should send a GET request to the Company Profile API

    Examples:
      | companyNumber | topicName              |
      | 00006400      | stream-company-insolvency |

