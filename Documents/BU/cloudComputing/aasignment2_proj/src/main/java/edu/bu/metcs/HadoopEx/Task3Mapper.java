package edu.bu.metcs.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task3Mapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create a counter and initialize with 1
	private final IntWritable moneyPerMin = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text driverID = new Text();
	private String[] tripArr = null;
	private String tripInfo = null;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString());
		int min = 0;
		int totalAmt = 0;

		while (itr.hasMoreTokens()) {
			tripInfo = itr.nextToken().trim();
			if (tripInfo != null) {
				tripArr = tripInfo.split(",");
				// array length 17 implies its correct and clean record
				if (tripArr.length >= 17) {
					// tripArr[5] is trip_distance
					if (!tripArr[5].isEmpty()) {
						if (Integer.parseInt(tripArr[5]) > 0) {
							System.out.println("no GPS erorr as distance is greater than 0");
							// trip_time_in_secs is tripArr[4]
							min = (Integer.parseInt(tripArr[4])) / 60;

							// total_amount is tripArr[16]
							totalAmt = Integer.parseInt(tripArr[16]);

							// average earned money per minute
							moneyPerMin.set(totalAmt / min);
							
							// driver id is tripArr[1]
							driverID.set(tripArr[1]);
							
							context.write(driverID, moneyPerMin);
						}
					}
				}
			}

		}
	}
}