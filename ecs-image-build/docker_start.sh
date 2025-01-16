#!/bin/bash
#
# Start script for company-links-consumer

PORT=8080
exec java -jar -Dserver.port="${PORT}" "company-links-consumer.jar"
