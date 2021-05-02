package com.example.batchApp;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Future;

@Slf4j
@Configuration
public class BatchAsyncProcessorConfig {

//    public static final String INPUT_FILE_DIRECTORY = "C:\\Users\\karth\\Downloads\\batchApp\\";
//    public static final String OUTPUT_FILE_DIRECTORY = "C:\\Users\\karth\\Downloads\\batchApp\\";
//    public static final String IN_JSON_FILE = "person.json";
//    public static final String OUT_FILE_NAME = "out.json";
//    private static final String JOB_NAME = "personJob";
//
//    private final JobBuilderFactory jobBuilders;
//    private final StepBuilderFactory stepBuilders;
//
//
//    public BatchAsyncProcessorConfig(JobBuilderFactory jobBuilders, StepBuilderFactory stepBuilders) {
//        this.jobBuilders = jobBuilders;
//        this.stepBuilders = stepBuilders;
//    }
//
//    @Bean
//    public Job personJob() throws Exception {
//        return jobBuilders.get("customerReportJob")
//                .start(taskletStep())
//                .next(chunkStep())
//                .build();
//    }
//
//    @Bean
//    public Step chunkStep() throws Exception {
//        return stepBuilders.get("chunkStep")
//                .listener(personStepListener())
//                .<Person, Person>chunk(100)
//                .reader(streamReader())
//                .processor(asyncItemProcessor())
//                .writer(asyncItemWriter())
//                .taskExecutor(taskExecutor())
//                .build();
//    }
//
//    @Bean
//    public TaskExecutor taskExecutor() {
//        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor("spring_batch");
//        asyncTaskExecutor.setConcurrencyLimit(10);
//        return asyncTaskExecutor;
//    }
//
//    public SynchronizedItemStreamReader streamReader() {
//        SynchronizedItemStreamReader itemStreamReader = new SynchronizedItemStreamReader();
//        itemStreamReader.setDelegate(reader());
//        return itemStreamReader;
//    }
//
//    @Bean
//    public ItemStreamReader<Person> reader() {
//        return new JsonItemReaderBuilder<Person>()
//                .name("personJsonItemReader")
//                .resource(new FileSystemResource(INPUT_FILE_DIRECTORY + IN_JSON_FILE))
//                .jsonObjectReader(new JacksonJsonObjectReader<>(Person.class))
//                .build();
//    }
//
//    @Bean
//    public JsonFileItemWriter<Person> jsonItemWriter() throws IOException {
//        JsonFileItemWriterBuilder<Person> builder = new JsonFileItemWriterBuilder<>();
//        JacksonJsonObjectMarshaller<Person> marshaller = new JacksonJsonObjectMarshaller<>();
//        return builder
//                .name("bookItemWriter")
//                .jsonObjectMarshaller(marshaller)
//                .resource(new FileSystemResource(OUTPUT_FILE_DIRECTORY + OUT_FILE_NAME))
//                .build();
//    }
//
//    @Bean
//    public AsyncItemProcessor asyncItemProcessor() throws Exception {
//        AsyncItemProcessor<Person, Person> asyncItemProcessor = new AsyncItemProcessor();
//        asyncItemProcessor.setDelegate(personListItemProcessor());
//        asyncItemProcessor.setTaskExecutor(new SimpleAsyncTaskExecutor());
//        asyncItemProcessor.afterPropertiesSet();
//        return asyncItemProcessor;
//    }
//
//    @Bean
//    public ItemWriter<Future<Person>> asyncItemWriter() throws IOException {
//        AsyncItemWriter<Person> asyncItemWriter = new AsyncItemWriter<>();
//        asyncItemWriter.setDelegate(jsonItemWriter());
//        return asyncItemWriter;
//    }
//
//
//    @Bean
//    public ItemProcessor personListItemProcessor() {
//        return person -> person;
//    }
//
//
//    @Bean
//    public Step taskletStep() {
//        return stepBuilders.get("taskletStep")
//                .tasklet(tasklet())
//                .build();
//    }
//
//    @Bean
//    public Tasklet tasklet() {
//        return (contribution, chunkContext) -> {
//            return RepeatStatus.FINISHED;
//        };
//    }
//
//    @Bean
//    public StepExecutionListener personStepListener() {
//        return new StepExecutionListener() {
//
//            @Override
//            public void beforeStep(StepExecution stepExecution) {
//                stepExecution.getExecutionContext().put("start",
//                        new Date().getTime());
//                System.out.println("Step name:" + stepExecution.getStepName()
//                        + " Started");
//            }
//
//            @Override
//            public ExitStatus afterStep(StepExecution stepExecution) {
//                long elapsed = new Date().getTime()
//                        - stepExecution.getExecutionContext().getLong("start");
//                System.out.println("Step name:" + stepExecution.getStepName()
//                        + " Ended. Running time is " + elapsed + " milliseconds.");
//                System.out.println("Read Count:" + stepExecution.getReadCount() +
//                        " Write Count:" + stepExecution.getWriteCount());
//                return ExitStatus.COMPLETED;
//            }
//        };
//    }
}
