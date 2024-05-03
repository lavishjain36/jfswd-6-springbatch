package com.guvi.springbatch.config;

import com.guvi.springbatch.entity.Customer;
import com.guvi.springbatch.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@AllArgsConstructor
public class SpringBatchConfig {
    //Define all using @Bean annotations and

    //inject customerepo into spring batch process
    private CustomerRepository customerRepository;


    //bean of FlatFiles for reading data from files
    @Bean
    public FlatFileItemReader<Customer> reader(){
//        initialize the reader
        FlatFileItemReader<Customer> itemReader=new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);//skip the first row
        itemReader.setLineMapper(lineMapper());//map line mapper

        return  itemReader;

    }


    //configuring the mapping lines of files data and map to customer object
    private LineMapper<Customer> lineMapper(){
        //initialize the line mapper
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();
        //initialize LineTokenizer
        DelimitedLineTokenizer lineTokenizer=new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id","firstName","lastName","email","gender","contactNo","country","dob");
        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);//set the filed set mapper for line mapper
        return  lineMapper;
    }

    //bean of processing customer object
    @Bean
    public  CustomerProcessor processor(){
        return  new CustomerProcessor();
    }


    //configure bean of itemWriter
    @Bean
    public RepositoryItemWriter<Customer> writer(){
        RepositoryItemWriter<Customer> writer=new RepositoryItemWriter<>();
        writer.setRepository(customerRepository);
        writer.setMethodName("save");
        return  writer;
    }

    //configure bean of Step and pass all the above things to step
    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager){
            return  new StepBuilder("csv-step",jobRepository).
                <Customer,Customer>chunk(10,transactionManager)
                    .reader((reader()))
                    .processor(processor())
                    .writer(writer())
                    .taskExecutor(taskExecutor())
                    .build();
    }

    @Bean
    public Job runJob(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new JobBuilder("importCustomers",jobRepository)
                .flow(step1(jobRepository,transactionManager)).end().build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        //create an object of simpleAsynTask
        SimpleAsyncTaskExecutor  asyncTaskExecutor=new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }



}
