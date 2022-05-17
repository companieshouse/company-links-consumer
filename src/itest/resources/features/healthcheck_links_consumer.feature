Feature: Healthcheck API endpoint

  Scenario: Client invokes GET /healthcheck endpoint
    Given Company links consumer api service is running
    When the client invokes '/company-links-consumer/healthcheck' endpoint
    Then the client receives a status code of 200
    And the client receives a response body of '{"status":"UP"}'