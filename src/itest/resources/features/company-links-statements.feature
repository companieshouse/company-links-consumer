Feature: Process company links information for statements

  Scenario: PATCH company statements link successfully - add link
    Given Company links consumer is available
    When  A valid "changed" message is consumed from the "statements" stream
    Then  A PATCH request is sent to the API
    And No messages are placed on the invalid, error or retry topics

  Scenario: Consume invalid statements message
    Given Company links consumer is available
    When An invalid message is consumed from the "statements" stream
    Then The message is placed on the "invalid" topic

  Scenario: Consume an statements message with an invalid event type
    Given Company links consumer is available
    When A message is consumed with invalid event type from the "statements" stream
    Then The message is placed on the "invalid" topic

  Scenario: Consume a valid statements message but the user is not authorized - add link
    Given Company links consumer is available
    And   The user is unauthorized
    When A valid "changed" message is consumed from the "statements" stream
    Then The message is placed on the "retry" topic
    And The message is placed on the "error" topic

  Scenario: Consume a valid statements message but the company profile api is unavailable - add link
    Given Company links consumer is available
    And   The company profile api is unavailable
    When A valid "changed" message is consumed from the "statements" stream
    Then The message is placed on the "retry" topic
    And The message is placed on the "error" topic

  Scenario: PATCH company statements link successfully - remove link
    Given Company links consumer is available
    And The number of "statements" remaining in the company is 0
    When  A valid "deleted" message is consumed from the "statements" stream
    Then  A PATCH request is sent to the API
    And No messages are placed on the invalid, error or retry topics


  Scenario: PATCH company statements link fails to delete statements - remove link
    Given Company links consumer is available
    And The number of "statements" remaining in the company is 2
    When  A valid "deleted" message is consumed from the "statements" stream
    Then  A PATCH request is NOT sent to the API
    And No messages are placed on the invalid, error or retry topics

  Scenario: Consume a valid statements message but the user is not authorized - remove link
    Given Company links consumer is available
    And   The user is unauthorized
    And The number of "statements" remaining in the company is 0
    When A valid "deleted" message is consumed from the "statements" stream
    Then The message is placed on the "invalid" topic

  Scenario: Consume a valid statements message but the company profile api is unavailable - remove link
    Given Company links consumer is available
    And   The company profile api is unavailable
    When A valid "deleted" message is consumed from the "statements" stream
    Then The message is placed on the "retry" topic
    And  The message is placed on the "error" topic