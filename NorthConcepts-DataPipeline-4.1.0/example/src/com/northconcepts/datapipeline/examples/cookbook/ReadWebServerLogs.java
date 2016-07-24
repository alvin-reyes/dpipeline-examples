/*
 * Copyright (c) 2006-2016 North Concepts Inc.  All rights reserved.
 * Proprietary and Confidential.  Use is subject to license terms.
 *
 * http://northconcepts.com/data-pipeline/licensing/
 *
 */
package com.northconcepts.datapipeline.examples.cookbook;

import java.io.File;

import com.northconcepts.datapipeline.core.DataReader;
import com.northconcepts.datapipeline.core.StreamWriter;
import com.northconcepts.datapipeline.job.Job;
import com.northconcepts.datapipeline.weblog.CombinedLogReader;

public class ReadWebServerLogs {
    
    public static void main(String[] args)  throws Throwable {
        DataReader reader = new CombinedLogReader(
                new File("example/data/input/localhost_access_log.2016-04-10.txt"));
        
        Job.run(reader, new StreamWriter(System.out));
    }

}
