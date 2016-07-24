/*
 * Copyright (c) 2006-2016 North Concepts Inc.  All rights reserved.
 * Proprietary and Confidential.  Use is subject to license terms.
 *
 * http://northconcepts.com/data-pipeline/licensing/
 *
 */
package com.areyes.sample;

import java.io.File;

import org.apache.log4j.Logger;

import com.northconcepts.datapipeline.core.DataEndpoint;
import com.northconcepts.datapipeline.core.DataReader;
import com.northconcepts.datapipeline.core.DataWriter;
import com.northconcepts.datapipeline.core.Messages;
import com.northconcepts.datapipeline.core.StreamWriter;
import com.northconcepts.datapipeline.csv.CSVReader;
import com.northconcepts.datapipeline.filter.FieldFilter;
import com.northconcepts.datapipeline.filter.Filter;
import com.northconcepts.datapipeline.filter.FilterExpression;
import com.northconcepts.datapipeline.filter.rule.IsJavaType;
import com.northconcepts.datapipeline.filter.rule.IsNotNull;
import com.northconcepts.datapipeline.filter.rule.PatternMatch;
import com.northconcepts.datapipeline.filter.rule.ValueMatch;
import com.northconcepts.datapipeline.job.Job;
import com.northconcepts.datapipeline.job.JobTemplate;
import com.northconcepts.datapipeline.validate.ValidatingReader;

public class SyntaxValidation {
    
    public static final Logger log = DataEndpoint.log; 

    public static void main(String[] args) throws Throwable {
        DataReader reader = new CSVReader(new File("src/account_info.csv"))
            .setFieldNamesInFirstRow(true);
        
        ValidatingReader validatingReader = new ValidatingReader(reader)
            .setExceptionOnFailure(false)
            .setRecordStackTraceInMessage(false);
        
        validatingReader.add(new FieldFilter("account_status")
                .addRule(new IsNotNull())
                .addRule(new IsJavaType(String.class))
                .addRule(new ValueMatch<String>("ACTIVE", "IN-PROCESS", "INACTIVE")));  
        
        validatingReader.add(new FieldFilter("account")
                .addRule(new IsNotNull())
                .addRule(new IsJavaType(String.class))
                .addRule(new PatternMatch("[0-10]*"))); 
        
        DataWriter writer = new StreamWriter(System.out);

        Job.run(validatingReader, writer);
        
        log.info("messages: " + Messages.getCurrent());
    }

}
