package com.northconcepts.datapipeline.examples.cookbook.blog;

import java.io.File;

import com.northconcepts.datapipeline.core.DataReader;
import com.northconcepts.datapipeline.core.DataWriter;
import com.northconcepts.datapipeline.csv.CSVReader;
import com.northconcepts.datapipeline.csv.CSVWriter;
import com.northconcepts.datapipeline.job.JobTemplate;
import com.northconcepts.datapipeline.transform.IncludeFields;
import com.northconcepts.datapipeline.transform.TransformingReader;

public class SelectivelyTransferCsvFile {

	public static void main(String[] args) {

		DataReader reader = new CSVReader(new File(
				"example/data/input/GeoliteDataInput.csv"))
				.setFieldNamesInFirstRow(false);

		TransformingReader transformingReader = new TransformingReader(reader);
		transformingReader.add(new IncludeFields("E", "F"));

		DataWriter writer = new CSVWriter(new File(
				"example/data/output/GeoliteDataOutput.csv"))
				.setFieldNamesInFirstRow(false);

		JobTemplate.DEFAULT.transfer(transformingReader, writer);

	}
}
