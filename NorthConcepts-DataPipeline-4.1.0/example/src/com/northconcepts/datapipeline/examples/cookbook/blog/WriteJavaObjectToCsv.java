package com.northconcepts.datapipeline.examples.cookbook.blog;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.northconcepts.datapipeline.core.DataWriter;
import com.northconcepts.datapipeline.csv.CSVWriter;
import com.northconcepts.datapipeline.javabean.JavaBeanReader;
import com.northconcepts.datapipeline.job.JobTemplate;

public class WriteJavaObjectToCsv {

    public static void main(String[] args) {

        List<Signal> messages = getTestData();

        JavaBeanReader reader = new JavaBeanReader("messages", messages);

        reader.addField("source", "//source");
        reader.addField("name", "//name");
        reader.addField("component", "//component");
        reader.addField("occurrence", "//occurrence");
        reader.addField("size", "//size");
        reader.addField("bandwidth", "//bandwidth");
        reader.addField("sizewithHeader", "//sizewithHeader");
        reader.addField("bandwidthWithHeader", "//bandwidthWithHeader");

        reader.addRecordBreak("//Signal");

        DataWriter writer = new CSVWriter(new File("example/data/output/output.csv"));

        JobTemplate.DEFAULT.transfer(reader, writer);
    }

    private static List<Signal> getTestData() {
        List<Signal> messages = new ArrayList<Signal>();

        messages.add(new Signal("TestSource1", "TestName1", "TestComponent1", 1, 1, 1.0f, 1, 1.0f));
        messages.add(new Signal("TestSource2", "TestName2", "TestComponent2", 2, 2, 2.0f, 2, 2.0f));

        return messages;
    }

}
