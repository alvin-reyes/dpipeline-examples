/*
 * Copyright (c) 2006-2016 North Concepts Inc.  All rights reserved.
 * Proprietary and Confidential.  Use is subject to license terms.
 *
 * http://northconcepts.com/data-pipeline/licensing/
 *
 */
package com.northconcepts.datapipeline.examples.cookbook.customization;

import com.northconcepts.datapipeline.core.DataReader;
import com.northconcepts.datapipeline.core.StreamWriter;
import com.northconcepts.datapipeline.job.JobTemplate;

public class WriteMyOwnDataReader {
    
    public static void main(String[] args) throws Throwable {
        DataReader reader = new MyDataReader();
        reader = new MyProxyReader(reader);
        JobTemplate.DEFAULT.transfer(reader, new StreamWriter(System.out));
    }
    
}
