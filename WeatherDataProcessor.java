import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WeatherDataProcessor {

    public static class WeatherMapper extends Mapper<Object, Text, Text, Text> {
        private Text date = new Text();
        private Text weatherMessage = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Validate the input format (Date, Temperature, Humidity, Condition)
            if (fields.length < 4) {
                return; // Skip invalid lines
            }

            String dateStr = fields[0].trim();
            int temperature;
            String condition = fields[3].trim().toLowerCase(); // Normalize condition to lowercase

            try {
                temperature = Integer.parseInt(fields[1].trim());
            } catch (NumberFormatException e) {
                return; // Skip invalid temperature values
            }

            // Classify weather conditions
            if (condition.contains("snow")) {
                weatherMessage.set("Snowy Day");
            } else if (condition.contains("storm")) {
                weatherMessage.set("Stormy Weather - Stay Safe!");
            } else if (condition.contains("rain")) {
                weatherMessage.set("Rainy Day");
            } else if (temperature >= 30) {
                weatherMessage.set("Hot Day");
            } else if (temperature < 10) {
                weatherMessage.set("Cold Day");
            } else {
                weatherMessage.set("Moderate Weather");
            }

            date.set(dateStr);
            context.write(date, weatherMessage);
        }
    }

    public static class WeatherReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Data Classification");

        job.setJarByClass(WeatherDataProcessor.class);
        job.setMapperClass(WeatherMapper.class);
        job.setCombinerClass(WeatherReducer.class);
        job.setReducerClass(WeatherReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
