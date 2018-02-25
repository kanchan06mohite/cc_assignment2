package edu.bu.metcs.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task2Mapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable noOfErrs = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text taxiId = new Text();
	private String[] tripArr = null;
	private String tripInfo = null;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString());

		while (itr.hasMoreTokens()) {
			tripInfo = itr.nextToken().trim();
			if (tripInfo != null) {
				tripArr = tripInfo.split(",");
				// array length 17 implies its correct and clean record
				if (tripArr.length >= 17) {
					// tripArr[5] is trip_distance
					// check if trip distance is provided
					if (!tripArr[5].isEmpty()) {
						if (Integer.parseInt(tripArr[5]) > 0)
							System.out.println("no GPS erorr as distance is greater than 0");
						else {
							// tripArr[0] is medallion which is taxi id
							taxiId.set(tripArr[0]);
							context.write(taxiId, noOfErrs);
						}
					}
				}
			}
		}
	}
}
