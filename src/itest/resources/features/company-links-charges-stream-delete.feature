Feature: Process company links charges stream delete scenarios


  Scenario Outline: Company profile exists with charges link but get charges returns empty list a kafka delete will patch the profile to remove the charges link

    Given Company links consumer api service is running
    And stubbed set with "<linksResponse>" and "<chargesResponse>" for "<companyNumber>"
    When A valid avro delete message for company number "<companyNumber>" is sent to the Kafka topic "<topicName>"
    Then The message is successfully consumed and company-profile-api PATCH endpoint is invoked removing charges link

    Examples:
      | companyNumber | topicName              | linksResponse                   | chargesResponse        |
      | 00006400      | stream-company-charges | profile-with-charges-links.json | no-charges-output.json |


  Scenario Outline: Company profile exists with out charges link and get charges returns empty list a kafka delete will not patch the profile

    Given Company links consumer api service is running
    And stubbed set with "<linksResponse>" and "<chargesResponse>" for "<companyNumber>"
    When A valid avro delete message for company number "<companyNumber>" is sent to the Kafka topic "<topicName>"
    Then The message is successfully consumed and company-profile-api PATCH endpoint is not invoked

    Examples:
      | companyNumber | topicName              | linksResponse                 | chargesResponse        |
      | 00006400      | stream-company-charges | profile-with-out-charges.json | no-charges-output.json |


  Scenario Outline: Company profile exists with charges link and get charges returns a list of links a kafka delete will not patch the profile

    Given Company links consumer api service is running
    And stubbed set with "<linksResponse>" and "<chargesResponse>" for "<companyNumber>"
    When A valid avro delete message for company number "<companyNumber>" is sent to the Kafka topic "<topicName>"
    Then The message is successfully consumed and company-profile-api PATCH endpoint is not invoked

    Examples:
      | companyNumber | topicName              | linksResponse                   | chargesResponse     |
      | 00006400      | stream-company-charges | profile-with-charges-links.json | charges-output.json |


  Scenario Outline: Company profile exists with charges link and get charges returns a 503 the kafka delete is retried 3 times then sent to error topic

    Given Company links consumer api service is running
    And stubbed set with "<linksResponse>" for "<companyNumber>" and getCharges give 503
    When A valid avro delete message for company number "<companyNumber>" is sent to the Kafka topic "<topicName>"
    Then The message fails to process and retrys 3 times bvefore being sent to the "<errorTopic>"

    Examples:
      | companyNumber | topicName              | linksResponse                   | errorTopic                                          |
      | 00006400      | stream-company-charges | profile-with-charges-links.json | stream-company-charges-company-links-consumer-error |


  Scenario Outline: Company profile exists with charges link and get charges returns a 404 the kafka delete is retried 3 times then sent to error topic

    Given Company links consumer api service is running
    And stubbed set with "<linksResponse>" for "<companyNumber>" and getCharges give 404
    When A valid avro delete message for company number "<companyNumber>" is sent to the Kafka topic "<topicName>"
    Then The message fails to process and retrys 3 times bvefore being sent to the "<errorTopic>"

    Examples:
      | companyNumber | topicName              | linksResponse                   | errorTopic                                          |
      | 00006400      | stream-company-charges | profile-with-charges-links.json | stream-company-charges-company-links-consumer-error |

  Scenario Outline: Company profile exists with charges link and get charges returns an empty list the kafka delete is processed and the patch returns 400

    Given Company links consumer api service is running
    And stubbed set with "<linksResponse>" and "<chargesResponse>" for "<companyNumber>" but patch enpoint give 400
    When A valid avro delete message for company number "<companyNumber>" is sent to the Kafka topic "<topicName>"
    Then The message fails to process and sent to the "<invalidTopic>"

    Examples:
      | companyNumber | topicName              | linksResponse                   | chargesResponse        | invalidTopic                                          |
      | 00006400      | stream-company-charges | profile-with-charges-links.json | no-charges-output.json | stream-company-charges-company-links-consumer-invalid |
