package com.example.batchApp;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("batch")
public class BatchController {

    private final JobLauncher jobLauncher;

    private final Job job;

    public BatchController(JobLauncher jobLauncher, Job job) {
        this.jobLauncher = jobLauncher;
        this.job = job;
    }

    @GetMapping("run")
    public void run() throws Exception {
        JobExecution execution = jobLauncher.run(
                job,
                new JobParametersBuilder().addLong("uniqueness", System.nanoTime()).toJobParameters()
        );
        log.info("Exit status: {}", execution.getStatus());
    }
}
