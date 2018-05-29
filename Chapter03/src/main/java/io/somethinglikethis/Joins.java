package io.somethinglikethis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Joins
{
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "City Temperature Job");
        job.setMapperClass(TemperatureMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CityMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TemperatureMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /*
    Id,City
    1,Boston
    2,New York
    */
    private static class CityMapper

            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String txt = value.toString();
            String[] tokens = txt.split(",");
            String id = tokens[0].trim();
            String name = tokens[1].trim();
            if (name.compareTo("City") != 0)
                context.write(new Text(id), new Text(name));
        }
    }

    /*
    Date,Id,Temperature
    2018-01-01,1,21
    2018-01-01,2,22
    */
    private static class TemperatureMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String txt = value.toString();
            String[] tokens = txt.split(",");
            String date = tokens[0];
            String id = tokens[1].trim();
            String temperature = tokens[2].trim();
            if (temperature.compareTo("Temperature") != 0)
                context.write(new Text(id), new Text(temperature));
        }
    }


    private static class TemperatureReducer
            extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text cityName = new Text("Unknown");
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            int n = 0;

            cityName = new Text("city-"+key.toString());

            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.length() <=3)
                {
                    sum += Integer.parseInt(strVal);
                    n +=1;
                } else {
                    cityName = new Text(strVal);
                }
            }
            if (n==0) n = 1;
            result.set(sum/n);
            context.write(cityName, result);
        }
    }

    private static class InnerJoinReducer
            extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text cityName = new Text("Unknown");
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            int n = 0;

            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.length() <=3)
                {
                    sum += Integer.parseInt(strVal);
                    n +=1;
                } else {
                    cityName = new Text(strVal);
                }
            }
            if (n!=0 && cityName.toString().compareTo("Unknown") !=0) {
                result.set(sum / n);
                context.write(cityName, result);
            }
        }
    }

    private static class LeftAntiJoinReducer
            extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text cityName = new Text("Unknown");
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            int n = 0;

            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.length() <=3)
                {
                    sum += Integer.parseInt(strVal);
                    n +=1;
                } else {
                    cityName = new Text(strVal);
                }
            }
            if (n==0 ) {
                if (n==0) n = 1;
                result.set(sum / n);
                context.write(cityName, result);
            }
        }
    }

    private static class LeftOuterJoinReducer
            extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text cityName = new Text("Unknown");
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            int n = 0;

            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.length() <=3)
                {
                    sum += Integer.parseInt(strVal);
                    n +=1;
                } else {
                    cityName = new Text(strVal);
                }
            }
            if (cityName.toString().compareTo("Unknown") !=0) {
                if (n==0) n = 1;
                result.set(sum / n);
                context.write(cityName, result);
            }
        }
    }

    private static class RightOuterJoinReducer
            extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text cityName = new Text("Unknown");
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            int n = 0;
            cityName = new Text("city-"+key.toString());
            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.length() <=3)
                {
                    sum += Integer.parseInt(strVal);
                    n +=1;
                } else {
                    cityName = new Text(strVal);
                }
            }
            if (n !=0) {
                result.set(sum / n);
                context.write(cityName, result);
            }
        }
    }

    private static class FullOuterJoinReducer
            extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text cityName = new Text("Unknown");
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            int n = 0;

            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.length() <=3)
                {
                    sum += Integer.parseInt(strVal);
                    n +=1;
                } else {
                    cityName = new Text(strVal);
                }
            }
            if (n==0) n = 1;
            result.set(sum/n);
            context.write(cityName, result);
        }
    }


}
