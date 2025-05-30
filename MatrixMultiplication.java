import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {

    public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String matrixName = tokens[0];  // Matrix name (A or B)
            int row = Integer.parseInt(tokens[1]);
            int col = Integer.parseInt(tokens[2]);
            int val = Integer.parseInt(tokens[3]);

            Configuration conf = context.getConfiguration();
            int commonDim = Integer.parseInt(conf.get("commonDim"));

            if (matrixName.equals("A")) {
                for (int k = 0; k < commonDim; k++) {
                    context.write(new Text(row + "," + k), new Text("A," + col + "," + val));
                }
            } else {
                for (int i = 0; i < commonDim; i++) {
                    context.write(new Text(i + "," + col), new Text("B," + row + "," + val));
                }
            }
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] A = new int[1000];  // Assuming max matrix size
            int[] B = new int[1000];

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts[0].equals("A")) {
                    A[Integer.parseInt(parts[1])] = Integer.parseInt(parts[2]);
                } else {
                    B[Integer.parseInt(parts[1])] = Integer.parseInt(parts[2]);
                }
            }

            int sum = 0;
            for (int i = 0; i < 1000; i++) {
                sum += A[i] * B[i];
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("commonDim", "2"); // Set common dimension (Adjust based on matrix size)

        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
