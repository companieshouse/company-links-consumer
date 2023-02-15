package uk.gov.companieshouse.company.links.consumer;

import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class ResettableCountDownLatch {

    private CountDownLatch countDownLatch;

    public void resetLatch(int count) {
        countDownLatch = new CountDownLatch(count);
    }

    public void countDown() {
        countDownLatch.countDown();
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public void countDownAll() {
        while (countDownLatch.getCount() > 0) {
            countDownLatch.countDown();
        }
    }
}
