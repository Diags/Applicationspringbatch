package springbatch.example.config;

import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;

import org.slf4j.Logger;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {
    private Date startTime, stopTime;


    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    @Override
    public void beforeJob(JobExecution jobExecution) {
        startTime = new Date();
        if (jobExecution.isRunning()) {
            log.debug(jobExecution.getJobId() + "est en cours ************");
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        stopTime = new Date();
        System.out.println("ExamResult Job stops at :" + stopTime);
        System.out.println("Total time take in millis :" + getTimeInMillis(startTime, stopTime));

        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            System.out.println("ExamResult job completed successfully");
            //Here you can perform some other business logic like cleanup
        } else if (jobExecution.getStatus() == BatchStatus.FAILED) {
            System.out.println("ExamResult job failed with following exceptions ");
            List<Throwable> exceptionList = jobExecution.getAllFailureExceptions();
            for (Throwable th : exceptionList) {
                System.err.println("exception :" + th.getLocalizedMessage());
            }
        }

    }

    private long getTimeInMillis(Date start, Date stop){
        return stop.getTime() - start.getTime();
    }

}
