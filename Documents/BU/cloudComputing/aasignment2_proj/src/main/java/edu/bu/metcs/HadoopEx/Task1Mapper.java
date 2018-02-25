package edu.bu.metcs.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1Mapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable noOfErrs = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text hourOfDay = new Text();
	private String[] tripArr = null;
	private String tripInfo = null;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString());

		while (itr.hasMoreTokens()) {
			tripInfo = itr.nextToken();
			if (tripInfo != null) {
				tripArr = tripInfo.split(",");
				// array length 17 implies its correct and clean record
				if (tripArr.length >= 6) {
					// tripArr[5] is trip_distance
					if (!tripArr[5].isEmpty()) {
						if (Double.parseDouble(tripArr[5]) > 0)
							System.out.println("no GPS erorr as distance is greater than 0");
						else {
							// tripArr[2] is pick up time
							// pick and drop off date time is formatted as
							// yyyy-mm-dd hh:mm:ss for e.g.'2013-01-0100:03:00'
							// after trimming 11th to 12th position is hour
							System.out.println("--------------------pick up date time len-------------------"
									+ tripArr[2].length());
							if (tripArr[2].length() >= 12) {
								hourOfDay.set(tripArr[2].substring(11, 12));
								context.write(hourOfDay, noOfErrs);
							}
						}
					}

				}
			}

		}
	}
}