import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
    	  Configuration conf = this.getConf();

          Job linkCountJob = Job.getInstance(conf, "Orphan Pages");
          linkCountJob.setOutputKeyClass(IntWritable.class);
          linkCountJob.setOutputValueClass(NullWritable.class);
          
          linkCountJob.setMapOutputKeyClass(IntWritable.class);
          linkCountJob.setMapOutputValueClass(IntWritable.class);

          linkCountJob.setMapperClass(LinkCountMap.class);
          linkCountJob.setReducerClass(OrphanPageReduce.class);

          FileInputFormat.setInputPaths(linkCountJob, new Path(args[0]));
          FileOutputFormat.setOutputPath(linkCountJob, new Path(args[1]));

          linkCountJob.setJarByClass(OrphanPages.class);
          return linkCountJob.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(":");
            //set the current page to have 0 links
            context.write(new IntWritable(Integer.parseInt(line[0].trim())), new IntWritable(0));
            StringTokenizer tokenizer = new StringTokenizer(line[1], " \t");

            //set all the links in the current pages to have 1 link
            while (tokenizer.hasMoreTokens()) {
                context.write(new IntWritable(Integer.parseInt(tokenizer.nextToken().trim())), new IntWritable(1));
            }
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for(IntWritable currentPage: values) {
                if(currentPage.get() >0) {
                    count++;
                }
            }
            if(count==0) {
                context.write(key, NullWritable.get());
            }
        }
    }
}