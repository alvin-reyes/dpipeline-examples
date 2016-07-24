package com.northconcepts.datapipeline.examples.userguide;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.northconcepts.datapipeline.core.AsyncMultiReader;
import com.northconcepts.datapipeline.core.AsyncReader;
import com.northconcepts.datapipeline.core.AsyncWriter;
import com.northconcepts.datapipeline.core.DataEndpoint;
import com.northconcepts.datapipeline.core.DataReader;
import com.northconcepts.datapipeline.core.DataWriter;
import com.northconcepts.datapipeline.core.MultiWriter;
import com.northconcepts.datapipeline.core.Record;
import com.northconcepts.datapipeline.core.StreamWriter;
import com.northconcepts.datapipeline.csv.CSVReader;
import com.northconcepts.datapipeline.csv.CSVWriter;
import com.northconcepts.datapipeline.filter.FilterExpression;
import com.northconcepts.datapipeline.filter.FilteringReader;
import com.northconcepts.datapipeline.group.CloseWindowStrategy;
import com.northconcepts.datapipeline.group.CreateWindowStrategy;
import com.northconcepts.datapipeline.group.GroupByReader;
import com.northconcepts.datapipeline.job.Job;
import com.northconcepts.datapipeline.job.JobCallback;
import com.northconcepts.datapipeline.job.JobLifecycleListener;
import com.northconcepts.datapipeline.job.JobTemplate;
import com.northconcepts.datapipeline.json.SimpleJsonWriter;
import com.northconcepts.datapipeline.multiplex.DeMux;
import com.northconcepts.datapipeline.test.common.SlowProxyReader;
import com.northconcepts.datapipeline.transform.BasicFieldTransformer;
import com.northconcepts.datapipeline.transform.SetCalculatedField;
import com.northconcepts.datapipeline.transform.TransformingReader;
import com.northconcepts.datapipeline.xml.SimpleXmlWriter;

public class RunningJobs {

    public static void main0(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/call-center-inbound-call.csv"))
            .setFieldNamesInFirstRow(true);
        
        DataWriter writer = new SimpleXmlWriter(new File("example/data/output/call-center-inbound-call.xml"))
            .setPretty(true);
        
        Job job = new Job(reader, writer);
        job.run();

        job.runAsync();
        
        new Thread(job).start();
        
        ExecutorService pool = Executors.newFixedThreadPool(10);
        pool.execute(job);
        Future<?> future = pool.submit(job);
        
        
        System.out.println("Job created");

        job.waitUntilStarted();
        System.out.println("Job started");
        
        job.waitUntilFinished();
        System.out.println("Job finished");
        
        JobTemplate.DEFAULT.transfer(reader, writer);
        
    }

    public static void main1(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/call-center-inbound-call.csv"))
            .setFieldNamesInFirstRow(true);
        
        DataWriter writer = new SimpleXmlWriter(new File("example/data/output/call-center-inbound-call.xml"))
            .setPretty(true);
        
        Job job = new Job(reader, writer);
        job.runAsync();
        
        Thread.sleep(1000L);

        job.cancel();
        job.waitUntilFinished();
        
        System.out.println("Job created on " + job.getCreatedOn());
        System.out.println("Job started on " + job.getStartedOn());
        System.out.println("Job cancelled on " + job.getCancelledOn());
        System.out.println("Job finished on " + job.getFinishedOn());
    }

    public static void main2(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/call-center-inbound-call.csv"))
            .setFieldNamesInFirstRow(true);
        
        DataWriter writer = new SimpleXmlWriter(new File("example/data/output/call-center-inbound-call.xml"))
            .setPretty(true);
        
        Job job = new Job(reader, writer);
        job.runAsync();
        
        Thread.sleep(10L);

        job.pause();
        System.out.println("Job paused on " + job.getPausedOn());
        Thread.sleep(1000L);
        job.resume();
        System.out.println("Job paused on " + job.getPausedOn());  // returns null if not paused

        job.waitUntilFinished();
        
        System.out.println("Job finished on " + job.getFinishedOn());
    }

    public static void main3(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/call-center-inbound-call.csv"))
            .setFieldNamesInFirstRow(true);
        
        DataWriter writer = new SimpleXmlWriter(new File("example/data/output/call-center-inbound-call.xml"))
            .setPretty(true);
        
        JobCallback<DataReader, DataWriter> callback = new JobCallback<DataReader, DataWriter>(){
            @Override
            public void onProgress(DataReader reader, DataWriter writer, Record currentRecord) throws Throwable {
                if (writer.getRecordCount() % 50 == 0) {
                    System.out.println("50");
                }
            }
            
            @Override
            public void onSuccess(DataReader reader, DataWriter writer) throws Throwable {
                System.out.println("success");
            }
            
            @Override
            public void onFailure(DataReader reader, DataWriter writer, Record currentRecord, Throwable exception) throws Throwable {
                System.out.println("failure: " + exception);
            }
        };
        
        Job job = new Job(reader, writer);
        job.setCallback(callback);
        job.runAsync();

        job.waitUntilFinished();
    }

    public static void main3b(String[] args) throws Throwable {
        JobLifecycleListener lifecycleListener = new JobLifecycleListener() {
            
            @Override
            public void onStart(Job job) {
                System.out.println("*****  job started: " + job.getName());
            }
            
            @Override
            public void onFinish(Job job) {
                System.out.println("*****  job finished: " + job.getName() + "  --  " + (job.isFailed()?"unsuccessfully":"successfully"));
            }
            
            @Override
            public void onPause(Job job) {
                System.out.println("*****  job paused: " + job.getName());
            }
            
            @Override
            public void onResume(Job job) {
                System.out.println("*****  job resumed: " + job.getName());
            }
        };
        
        Job.addJobLifecycleListener(lifecycleListener);
        
        
        DataReader reader = new CSVReader(
                new File("example/data/input/call-center-inbound-call.csv"))
                .setFieldNamesInFirstRow(true);
        
        reader = new SlowProxyReader(reader, 1000L, 0);
        
        DataWriter writer = new SimpleXmlWriter(
                new File("example/data/output/call-center-inbound-call.xml"))
                .setPretty(true);
        
        
        Job job = new Job(reader, writer);
        job.runAsync();
        
        Thread.sleep(1000L);
        job.pause();
        Thread.sleep(1000L);
        job.resume();
        Thread.sleep(1000L);
        job.cancel();

        job.waitUntilFinished();
    }

    public static void main4(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/call-center-inbound-call.csv"))
            .setFieldNamesInFirstRow(true);
        
        DataWriter writer = new SimpleXmlWriter(new File("example/data/output/call-center-inbound-call.xml"))
            .setPretty(true);
        
        DataWriter logWriter = new StreamWriter(System.out);
        
        Job job = new Job(reader, writer);
        job.setLogWriter(logWriter);
        job.runAsync();

        job.waitUntilFinished();
    }

    public static void main5(String[] args) throws Throwable {
        DataEndpoint.enableJmx();
        
        DataReader reader = new CSVReader(new File("example/data/input/call-center-inbound-call.csv"))
            .setFieldNamesInFirstRow(true);
        
        DataWriter writer = new SimpleXmlWriter(new File("example/data/output/call-center-inbound-call.xml"))
            .setPretty(true);
        
        reader = new SlowProxyReader(reader, 1000L, 0);
        
//        reader = new SlowProxyReader(reader, 500L, 0);
//        reader = new FilteringReader(reader);
//        reader = new ValidatingReader(reader);
//        reader = new TransformingReader(reader);
//        reader = new MeteredReader(reader);
//        reader = new TransformingReader(reader);
//        
//        reader = new SlowProxyReader(reader, 500L, 0);
//        reader = new FilteringReader(reader);
//        reader = new ValidatingReader(reader);
//        reader = new TransformingReader(reader);
//        reader = new MeteredReader(reader);
//        reader = new TransformingReader(reader);
        
        Job job = new Job(reader, writer);
        job.runAsync();

        job.waitUntilFinished();
        
//        Thread.sleep(1000L * 60L * 5L);
    }

    public static void main6(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/purchases.csv"))
            .setFieldNamesInFirstRow(true);

        // Convert strings to numbers
        reader = new TransformingReader(reader)
            .add(new BasicFieldTransformer("year").stringToInt())
            .add(new BasicFieldTransformer("month").stringToInt())
            .add(new BasicFieldTransformer("unit_price").stringToDouble())
            .add(new BasicFieldTransformer("qty").stringToInt())
            .add(new BasicFieldTransformer("total").stringToDouble())
            ;
    
        // Group by year and month
        reader = new GroupByReader(reader, "year", "month")
            .count("transactions")
            .sum("total", "total")
            .sum("qty", "total_qty")
            .avg("total", "avg_purchase")
            .min("total", "min_purchase")
            .max("total", "max_purchase")
            ;
        
        DataWriter writer = new StreamWriter(System.out);
        
        Job job = new Job(reader, writer);
        job.run();

    }

    public static void main7(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/purchases.csv"))
            .setFieldNamesInFirstRow(true);

        // Convert strings to numbers
        reader = new TransformingReader(reader)
            .add(new BasicFieldTransformer("year").stringToInt())
            .add(new BasicFieldTransformer("month").stringToInt())
            .add(new BasicFieldTransformer("unit_price").stringToDouble())
            .add(new BasicFieldTransformer("qty").stringToInt())
            .add(new BasicFieldTransformer("total").stringToDouble())
            ;
    
        // Group by year and month
        reader = new GroupByReader(reader, "year", "month")
            .count("transactions")
            .sum("total", "total")
            .sum("qty", "total_qty")
            .avg("total", "avg_purchase")
            .min("total", "min_purchase")
            .max("total", "max_purchase")
            .setCreateWindowStrategy(CreateWindowStrategy.limitOpened(1))
            .setCloseWindowStrategy(CloseWindowStrategy.limitedRecords(50))
            .setDebug(true)
            ;
        
        DataWriter writer = new StreamWriter(System.out);
        
        Job job = new Job(reader, writer);
        job.run();

    }

    public static void main8(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/purchases.csv"))
            .setFieldNamesInFirstRow(true);

        // Convert strings to numbers
        reader = new TransformingReader(reader)
            .add(new BasicFieldTransformer("year").stringToInt())
            .add(new BasicFieldTransformer("month").stringToInt())
            .add(new BasicFieldTransformer("unit_price").stringToDouble())
            .add(new BasicFieldTransformer("qty").stringToInt())
            .add(new BasicFieldTransformer("total").stringToDouble())
            ;
    
        // Group by year and month
        reader = new GroupByReader(reader, "year", "month")
            .count("transactions")
            .sum("total", "total")
            .sum("qty", "total_qty")
            .avg("total", "avg_purchase")
            .min("total", "min_purchase")
            .max("total", "max_purchase")
            .setCreateWindowStrategy(CreateWindowStrategy.recordPeriod(10))
            .setCloseWindowStrategy(CloseWindowStrategy.limitedRecords(40))
            .setDebug(true)
            ;
        
        DataWriter writer = new StreamWriter(System.out);
        
        Job job = new Job(reader, writer);
        job.run();

    }

    public static void main9(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/purchases.csv"))
            .setFieldNamesInFirstRow(true);

        // Convert strings to numbers
        reader = new TransformingReader(reader)
            .add(new BasicFieldTransformer("year").stringToInt())
            .add(new BasicFieldTransformer("month").stringToInt())
            .add(new BasicFieldTransformer("unit_price").stringToDouble())
            .add(new BasicFieldTransformer("qty").stringToInt())
            .add(new BasicFieldTransformer("total").stringToDouble())
            ;
    
        // Group by year and month
        reader = new GroupByReader(reader, "year", "month")
            .count("transactions")
            .sum("total", "total")
            .sum("qty", "total_qty")
            .avg("total", "avg_purchase")
            .min("total", "min_purchase")
            .max("total", "max_purchase")
            .setCreateWindowStrategy(CreateWindowStrategy.recordPeriod(40))
            .setCloseWindowStrategy(CloseWindowStrategy.limitedRecords(10))
            .setDebug(true)
            ;
        
        DataWriter writer = new StreamWriter(System.out);
        
        Job job = new Job(reader, writer);
        job.run();

    }
    
    public static void main10(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/purchases.csv"))
            .setFieldNamesInFirstRow(true);

        // Convert strings to numbers
        reader = new TransformingReader(reader)
            .add(new BasicFieldTransformer("year").stringToInt())
            .add(new BasicFieldTransformer("month").stringToInt())
            .add(new BasicFieldTransformer("unit_price").stringToDouble())
            .add(new BasicFieldTransformer("qty").stringToInt())
            .add(new BasicFieldTransformer("total").stringToDouble())
            ;
        reader = new SlowProxyReader(reader, 1000L, 0);
    
        // Group by year and month
        reader = new GroupByReader(reader, "year", "month")
            .count("transactions")
            .sum("total", "total")
            .sum("qty", "total_qty")
            .avg("total", "avg_purchase")
            .min("total", "min_purchase")
            .max("total", "max_purchase")
            
            .setCreateWindowStrategy(CreateWindowStrategy.startInterval(TimeUnit.HOURS.toMillis(1)))            
            .setCloseWindowStrategy(CloseWindowStrategy.or(
                            CloseWindowStrategy.limitedRecords(100000),
                            CloseWindowStrategy.limitedTime(TimeUnit.MINUTES.toMillis(15))))
                    
//                    .setCreateWindowStrategy(CreateWindowStrategy.startInterval(TimeUnit.HOURS.toMillis(1)))            
//                    .setCloseWindowStrategy(CloseWindowStrategy.limitedRecords(100000))
                    
            
//            .setCreateWindowStrategy(CreateWindowStrategy.limitOpened(1))
//            .setCloseWindowStrategy(CloseWindowStrategy.limitedTime(TimeUnit.MINUTES.toMillis(10)))
            
            .setCreateWindowStrategy(CreateWindowStrategy.limitOpened(1))
            .setCloseWindowStrategy(CloseWindowStrategy.limitedTime(TimeUnit.MINUTES.toMillis(10)))
            
//            .setCloseWindowStrategy(CloseWindowStrategy.limitedTime(10000L))
            .setDebug(true)
            ;
        
        DataWriter writer = new StreamWriter(System.out);
        
        Job job = new Job(reader, writer);
        job.run();

    }
    
    public static void main11(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/purchases.csv"))
            .setFieldNamesInFirstRow(true);

        // Convert strings to numbers
        reader = new TransformingReader(reader)
            .add(new BasicFieldTransformer("year").stringToInt())
            .add(new BasicFieldTransformer("month").stringToInt())
            .add(new BasicFieldTransformer("unit_price").stringToDouble())
            .add(new BasicFieldTransformer("qty").stringToInt())
            .add(new BasicFieldTransformer("total").stringToDouble())
            ;
    
        // One-to-many stream splitter/cloner
        DeMux splitter = new DeMux(reader, DeMux.Strategy.BROADCAST);

        // Detail job
        DataReader detailReader = splitter.createReader();
        DataWriter detailWriter = new SimpleXmlWriter(
                new File("example/data/output/purchases-detail.xml")).setPretty(true);
        Job detailJob = new Job(detailReader, detailWriter);
        detailJob.runAsync();

        // Summary job
        DataReader summaryReader = splitter.createReader();
        summaryReader = new GroupByReader(summaryReader, "year", "month")
            .count("transactions")
            .sum("total", "total")
            .sum("qty", "total_qty")
            .avg("total", "avg_purchase")
            .min("total", "min_purchase")
            .max("total", "max_purchase")
            ;
        DataWriter summaryWriter = new SimpleJsonWriter(
                new File("example/data/output/purchases-sumary.json")).setPretty(true);
        Job summaryJob = new Job(summaryReader, summaryWriter);
        summaryJob.runAsync();
        
        splitter.run();
    }
    
    public static void main12(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/purchases.csv"))
                .setFieldNamesInFirstRow(true);
        
        reader = new FilteringReader(reader)
                .add(new FilterExpression("product_name == 'Teddy Bear'"));

        reader = new TransformingReader(reader)
                .add(new BasicFieldTransformer("total").stringToBigDecimal())
                .add(new SetCalculatedField("discount", "total * 0.1"))
                ;
    
        DataWriter writer = new StreamWriter(System.out);
        
        Job job = new Job(reader, writer);
        job.run();
    }

    public static void main13(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/purchases.csv"))
                .setFieldNamesInFirstRow(true);
        
        reader = new AsyncReader(reader)
                .setMaxBufferSizeInBytes(1024 * 1024);
        
        reader = new FilteringReader(reader)
                .add(new FilterExpression("product_name == 'Teddy Bear'"));

        reader = new TransformingReader(reader)
                .add(new BasicFieldTransformer("total").stringToBigDecimal())
                .add(new SetCalculatedField("discount", "total * 0.1"))
                ;
    
        DataWriter writer = new StreamWriter(System.out);
        
        Job job = new Job(reader, writer);
        job.run();
    }

    public static void main14(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/purchases.csv"))
                .setFieldNamesInFirstRow(true);
        
        reader = new AsyncReader(reader)
                .setMaxBufferSizeInBytes(1024 * 1024);
        
        reader = new FilteringReader(reader)
                .add(new FilterExpression("product_name == 'Teddy Bear'"));

        reader = new AsyncReader(reader)
                .setMaxBufferSizeInBytes(1024 * 1024);
        
        reader = new TransformingReader(reader)
                .add(new BasicFieldTransformer("total").stringToBigDecimal())
                .add(new SetCalculatedField("discount", "total * 0.1"))
                ;
    
        DataWriter writer = new StreamWriter(System.out);
        
        Job job = new Job(reader, writer);
        job.run();
    }

    public static void main15(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/purchases.csv"))
                .setFieldNamesInFirstRow(true);
        
        reader = new FilteringReader(reader)
                .add(new FilterExpression("product_name == 'Teddy Bear'"));

        reader = new TransformingReader(reader)
                .add(new BasicFieldTransformer("total").stringToBigDecimal())
                .add(new SetCalculatedField("discount", "total * 0.1"))
                ;
    
        DataWriter writer = new StreamWriter(System.out);

        writer = new AsyncWriter(writer, 1000);
        
        Job job = new Job(reader, writer);
        job.run();
    }

    public static void main16(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/purchases.csv"))
            .setFieldNamesInFirstRow(true);

        DataWriter writer = new MultiWriter(
                new SimpleXmlWriter(new File("example/data/output/purchases-detail.xml")),
                new SimpleJsonWriter(new File("example/data/output/purchases-detail.json"))
                );
                
        new Job(reader, writer).run();
    }
    
    public static void main17(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/purchases.csv"))
            .setFieldNamesInFirstRow(true);

        DataWriter writer = new MultiWriter(
                new SimpleXmlWriter(new File("example/data/output/purchases-detail.xml")),
                new SimpleJsonWriter(new File("example/data/output/purchases-detail.json"))
                );
        
        writer = new AsyncWriter(writer);
                
        new Job(reader, writer).run();
    }
    
    public static void main18(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("example/data/input/purchases.csv"))
            .setFieldNamesInFirstRow(true);

        DataWriter writer = new MultiWriter(
                new AsyncWriter(new SimpleXmlWriter(new File("example/data/output/purchases-detail.xml"))),
                new AsyncWriter(new SimpleJsonWriter(new File("example/data/output/purchases-detail.json")))
                );
                
        new Job(reader, writer).run();
    }
    
    public static void main(String[] args) throws Throwable {
        DataReader reader = new AsyncMultiReader(
                new CSVReader(new File("example/data/input/call-center-inbound-call.csv"))
                    .setFieldNamesInFirstRow(true),
                new CSVReader(new File("example/data/input/call-center-inbound-call-2.csv"))
                    .setFieldNamesInFirstRow(true)
                );

        DataWriter writer = new StreamWriter(System.out);
        
        new Job(reader, writer).run();
    }
    
    
    
}
