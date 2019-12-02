package springbatch.example.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import springbatch.example.model.Customer;

import java.util.Comparator;


@Configuration
@EnableBatchProcessing
public class BatchConfig {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Value("classpath*:/in/*/*.csv")
    private Resource[] resources;

    @Bean

    public FlatFileItemReader<Customer> flatFileItemReader() {
        FlatFileItemReader<Customer> customerFlatFileItemReader = new FlatFileItemReader<>();
        customerFlatFileItemReader.setLinesToSkip(1);
        customerFlatFileItemReader.setLineMapper(customerLineMapper());
        return customerFlatFileItemReader;

    }

    @Bean
    public LineMapper<Customer> customerLineMapper() {
        DefaultLineMapper<Customer> customerDefaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter(",");
        delimitedLineTokenizer.setStrict(false);
        delimitedLineTokenizer.setNames(new String[]{"id","name"});
        customerDefaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
        BeanWrapperFieldSetMapper<Customer> customerBeanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
        customerBeanWrapperFieldSetMapper.setTargetType(Customer.class);
        customerDefaultLineMapper.setFieldSetMapper(customerBeanWrapperFieldSetMapper);
        return customerDefaultLineMapper;
    }

    @Bean
    public MultiResourceItemReader<Customer> customerMultiResourceItemReader() {
        MultiResourceItemReader<Customer> multiResourceItemReader = new MultiResourceItemReader<>();
        multiResourceItemReader.setResources(resources);
        multiResourceItemReader.setComparator(new Comparator<Resource>() {

            /** Compares resource descriptions. */
            @Override
            public int compare(Resource r1, Resource r2) {
                return r1.getDescription().compareTo(r2.getDescription());
            }
        });
        multiResourceItemReader.setDelegate(flatFileItemReader());
        return multiResourceItemReader;
    }


    @Bean
    public FlatFileItemWriter<Customer> writer() {
        //Create writer instance
        FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<>();
        //Set output file location
        writer.setResource(new ClassPathResource("/out/outputData.csv"));
        //All job repetitions should "append" to same output file
        writer.setAppendAllowed(true);
        //Name field values sequence based on object properties
        writer.setLineAggregator(new DelimitedLineAggregator<Customer>() {
            {
                setDelimiter(",");
                setFieldExtractor(new BeanWrapperFieldExtractor<Customer>() {
                    {
                        setNames(new String[]{"id", "name"});
                    }
                });
            }
        });
        return writer;
    }


    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<Customer, Customer>chunk(10)
                .reader(customerMultiResourceItemReader())
                .writer(writer())
                .build();
    }

    @Bean
    public Job mainJob() {

        return jobBuilderFactory.get("ETL-Load")
                .incrementer(new RunIdIncrementer())
                .flow(step1())
                .end()
                .build();
    }




}
