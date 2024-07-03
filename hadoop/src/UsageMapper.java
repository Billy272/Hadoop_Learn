import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UsageMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private Text year = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Assuming the date is in the first column and in the format "yyyy-MM-dd"
        String[] fields = line.split(",");
        if (fields.length > 0) {
            String date = fields[0];
            String yearStr = date.substring(0, 4);
            year.set(yearStr);
            context.write(year, one);
        }
    }
}
