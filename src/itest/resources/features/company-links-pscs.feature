Feature: Process company links information for pscs

  Scenario: PATCH company pscs link successfully - add link
    Given Company links consumer is available
    When  A valid "changed" message is consumed from the "pscs" stream
    Then  A PATCH request is sent to the API
    And No messages are placed on the invalid, error or retry topics

  Scenario: Consume invalid pscs message
    Given Company links consumer is available
    When An invalid message is consumed from the "pscs" stream
    Then The message is placed on the "invalid" topic

  Scenario: Consume an pscs message with an invalid event type
    Given Company links consumer is available
    When A message is consumed with invalid event type from the "pscs" stream
    Then The message is placed on the "invalid" topic

  Scenario: Consume a valid pscs message but the user is not authorized - add link
    Given Company links consumer is available
    And   The user is unauthorized
    When A valid "changed" message is consumed from the "pscs" stream
    Then The message is placed on the "retry" topic
    And The message is placed on the "error" topic

  Scenario: Consume a valid pscs message but the company profile api is unavailable - add link
    Given Company links consumer is available
    And   The company profile api is unavailable
    When A valid "changed" message is consumed from the "pscs" stream
    Then The message is placed on the "retry" topic
    And The message is placed on the "error" topic

  Scenario: PATCH company pscs link successfully - remove link
    Given Company links consumer is available
    And The number of "pscs" remaining in the company is 0
    When  A valid "deleted" message is consumed from the "pscs" stream
    Then  A PATCH request is sent to the API
    And No messages are placed on the invalid, error or retry topics


  Scenario: PATCH company pscs link fails to delete pscs - remove link
    Given Company links consumer is available
    And The number of "pscs" remaining in the company is 2
    When  A valid "deleted" message is consumed from the "pscs" stream
    Then  A PATCH request is NOT sent to the API
    And No messages are placed on the invalid, error or retry topics

  Scenario: Consume a valid pscs message but the user is not authorized - remove link
    Given Company links consumer is available
    And   The user is unauthorized
    And The number of "pscs" remaining in the company is 0
    When A valid "deleted" message is consumed from the "pscs" stream
    Then The message is placed on the "invalid" topic

  Scenario: Consume a valid pscs message but the company profile api is unavailable - remove link
    Given Company links consumer is available
    And   The company profile api is unavailable
    When A valid "deleted" message is consumed from the "pscs" stream
    Then The message is placed on the "retry" topic
    And  The message is placed on the "error" topic