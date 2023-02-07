Feature: Process company links information for exemptions

  Scenario Outline: PATCH company exemptions link successfully
    Given Company links consumer is available
    And   The response code 200 will be returned from the PATCH request for "<company_number>"
    When  A valid message is consumed for "<company_number>"
    Then  A PATCH request is sent to the add company exemptions link endpoint for "<company_number>"

    Examples:
      | company_number |
      | 00006400       |

  Scenario Outline: Consume invalid exemptions message
    Given Company links consumer is available
    And   The response code 200 will be returned from the PATCH request for "<company_number>"
    When An invalid message is consumed
    Then The message is placed on the appropriate topic: "<topic>"
    Examples:
      | company_number | topic   |
      | 00006400       | invalid |

  Scenario Outline: Consume an exemptions message with an invalid event type
    Given Company links consumer is available
    And   The response code 200 will be returned from the PATCH request for "<company_number>"
    When A message is consumed for "<company_number>" with invalid event type
    Then The message is placed on the appropriate topic: "<topic>"

    Examples:
      | company_number | topic   |
      | 00006400       | invalid |

  Scenario Outline: Consume a valid exemptions message but are not authorized
    Given Company links consumer is available
    And   The response code 401 will be returned from the PATCH request for "<company_number>"
    When A valid message is consumed for "<company_number>"
    Then The message is placed on the appropriate topic: "<topic>"

    Examples:
      | company_number | topic   |
      | 00006400       | invalid |

  Scenario Outline: Consume a valid exemptions message but company profile api is unavailable
    Given Company links consumer is available
    And   The response code 503 will be returned from the PATCH request for "<company_number>"
    When A valid message is consumed for "<company_number>"
    Then The message is placed on the appropriate topic: "<topic>"

    Examples:
      | company_number | topic   |
      | 00006400       | retry |