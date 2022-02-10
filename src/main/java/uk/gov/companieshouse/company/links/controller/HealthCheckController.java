package uk.gov.companieshouse.company.links.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthCheckController {

    /**
     * healthcheck.
     */
    @GetMapping("/healthcheck")
    public ResponseEntity<Void> healthcheck() {
        //TODO: Introduce spring actuator
        return ResponseEntity.status(HttpStatus.OK).build();
    }

}
