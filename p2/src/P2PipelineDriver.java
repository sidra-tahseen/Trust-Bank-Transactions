package p2;

import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class P2PipelineDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println(
                "Usage: P2PipelineDriver <input> <filtered-output> <branch-count-output>"
            );
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(
            conf,
            "P2 Pipeline - Job 1: Filter High Value Transactions"
        );

        job1.setJarByClass(P2PipelineDriver.class);

        job1.setMapperClass(HighValueMapper.class);
        job1.setReducerClass(HighValueReducer.class);

        job1.setMapOutputKeyClass(NullWritable.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(
            job1,
            new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
            job1,
            new Path(args[1])
        );

        Job job2 = Job.getInstance(
            conf,
            "P2 Pipeline - Job 2: Branch-wise High Value Count"
        );

        job2.setJarByClass(P2PipelineDriver.class);

        job2.setMapperClass(BranchCountMapper.class);
        job2.setReducerClass(BranchCountReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(
            job2,
            new Path(args[1])
        );

        FileOutputFormat.setOutputPath(
            job2,
            new Path(args[2])
        );

        ControlledJob controlledJob1 =
            new ControlledJob(
                job1,
                new ArrayList<ControlledJob>()
            );

        ControlledJob controlledJob2 =
            new ControlledJob(
                job2,
                new ArrayList<ControlledJob>()
            );

        controlledJob2.addDependingJob(controlledJob1);

        JobControl jobControl =
            new JobControl("P2 Pipeline");

        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        Thread controlThread =
            new Thread(jobControl);

        controlThread.start();

        while (!jobControl.allFinished()) {
            Thread.sleep(1000);
        }

        jobControl.stop();
        controlThread.join();

        if (!jobControl.getFailedJobList().isEmpty()) {
            System.err.println("P2 Pipeline FAILED.");
            System.exit(1);
        }

        System.out.println("P2 Pipeline completed successfully.");
    }
}
