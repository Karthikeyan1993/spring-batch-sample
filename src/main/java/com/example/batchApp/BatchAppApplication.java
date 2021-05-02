package com.example.batchApp;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@EnableBatchProcessing
@SpringBootApplication
public class BatchAppApplication {

	public static void main(String[] args) {
		SpringApplication.run(BatchAppApplication.class, args);
	}

}
