package com.example.batchApp;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Date;

@Slf4j
@Configuration
public class BatchConfig {
    public static final String TASKLET_STEP = "taskletStep";
    public static final String INPUT_FILE_DIRECTORY = "C:\\Users\\karth\\Downloads\\batchApp\\";
    public static final String OUTPUT_FILE_DIRECTORY = "C:\\Users\\karth\\Downloads\\batchApp\\";
    public static final String IN_JSON_FILE = "person.json";
    public static final String OUT_FILE_NAME = "out.json";
    private static final String JOB_NAME = "personJob";

    private final JobBuilderFactory jobBuilders;
    private final StepBuilderFactory stepBuilders;
    private final JobExplorer jobs;

    public BatchConfig(JobBuilderFactory jobBuilders, StepBuilderFactory stepBuilders, JobExplorer jobs) {
        this.jobBuilders = jobBuilders;
        this.stepBuilders = stepBuilders;
        this.jobs = jobs;
    }

    @PreDestroy
    public void destroy() throws NoSuchJobException {
        jobs.getJobNames().forEach(name -> log.info("job name: {}", name));
        jobs.getJobInstances(JOB_NAME, 0, jobs.getJobInstanceCount(JOB_NAME)).forEach(
                jobInstance -> log.info("job instance id {}", jobInstance.getInstanceId())
        );
    }

    @Bean
    public Job personJob() throws Exception {
        return jobBuilders.get(JOB_NAME)
                .start(taskletStep())
                .next(chunkStep())
                .build();
    }

    @Bean
    public Step taskletStep() {
        return stepBuilders.get(TASKLET_STEP)
                .tasklet(tasklet())
                .build();
    }

    @Bean
    public Step chunkStep() throws Exception {
        return stepBuilders.get("chunkStep")
                .listener(personStepListener())
                .<Person, Person>chunk(100)
                .reader(streamReader())
                .processor(personListItemProcessor())
                .writer(jsonItemWriter())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor("spring_batch");
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }


    public SynchronizedItemStreamReader streamReader() {
        SynchronizedItemStreamReader itemStreamReader = new SynchronizedItemStreamReader();
        itemStreamReader.setDelegate(reader());
        return itemStreamReader;
    }

    @Bean
    public ItemStreamReader<Person> reader() {
        return new JsonItemReaderBuilder<Person>()
                .name("personJsonItemReader")
                .resource(new FileSystemResource(INPUT_FILE_DIRECTORY + IN_JSON_FILE))
                .jsonObjectReader(new JacksonJsonObjectReader<>(Person.class))
                .build();
    }

    @Bean
    public ItemProcessor personListItemProcessor() {
        return person -> person;
    }

    @Bean
    public JsonFileItemWriter<Person> jsonItemWriter() throws IOException {
        JsonFileItemWriterBuilder<Person> builder = new JsonFileItemWriterBuilder<>();
        JacksonJsonObjectMarshaller<Person> marshaller = new JacksonJsonObjectMarshaller<>();
        return builder
                .name("bookItemWriter")
                .jsonObjectMarshaller(marshaller)
                .resource(new FileSystemResource(OUTPUT_FILE_DIRECTORY + OUT_FILE_NAME))
                .build();
    }

    @Bean
    public Tasklet tasklet() {
        return (contribution, chunkContext) -> {
            log.info("Executing tasklet step");
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public StepExecutionListener personStepListener() {
        return new StepExecutionListener() {

            @Override
            public void beforeStep(StepExecution stepExecution) {
                stepExecution.getExecutionContext().put("start",
                        new Date().getTime());
                System.out.println("Step name:" + stepExecution.getStepName()
                        + " Started");
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                long elapsed = new Date().getTime()
                        - stepExecution.getExecutionContext().getLong("start");
                System.out.println("Step name:" + stepExecution.getStepName()
                        + " Ended. Running time is " + elapsed + " milliseconds.");
                System.out.println("Read Count:" + stepExecution.getReadCount() +
                        " Write Count:" + stepExecution.getWriteCount());
                return ExitStatus.COMPLETED;
            }
        };
    }


}
