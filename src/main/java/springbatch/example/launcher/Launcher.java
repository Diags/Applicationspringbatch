package springbatch.example.launcher;

import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class Launcher {
    @Autowired
    private Job job;
    @Autowired
    private JobLauncher jobLauncher;

    @GetMapping("/launchjob")
    public BatchStatus launchJob() throws Exception{
        Map<String,JobParameter> jobParameterMap = new HashMap<>();
        jobParameterMap.put("time",new JobParameter(System.currentTimeMillis()));
        JobParameters jobParameters = new JobParameters(jobParameterMap);
        JobExecution jobExecution = jobLauncher.run(job, jobParameters);
        return jobExecution.getStatus();
    }
}
