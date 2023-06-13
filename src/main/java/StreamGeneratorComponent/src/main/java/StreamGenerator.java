package StreamGeneratorComponent.src.main.java;

import CustomDataTypes.*;
import Utils.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Properties;


public class StreamGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(StreamGenerator.class);

    static String pwd = Paths.get("").toAbsolutePath().toString();


    public static void main(String[] args) throws Exception{
        // Arg parsing
        OptionsGenerator options = new OptionsGenerator();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        // Excecution environment details //
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamFactory streamFactory = new StreamFactory(env);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", options.getKafkaURL());

        String leftOutputTopic = "pipeline-in-left";
        String rightOutputTopic = "pipeline-in-right";
        env.setMaxParallelism(128);
        env.setParallelism(1);

        LOG.info("Enter main.");

        DataStream<InputTuple> firstStream = streamFactory
                .createDataStream(options.getFirstStream(), options.getDelay(), options.getDuration(), options.getRate(),
                        options.getMinio(), LOG, options.getDimensions(), options.getEmbeddingsFile(), options.getDataset(),
                        options.getSleepsPerSecond(), options.getSleepTime(), options.getSeed());
        FlinkKafkaProducer<InputTuple> leftProducer = new FlinkKafkaProducer<>(
                leftOutputTopic,
                new TypeInformationSerializationSchema<>(TypeInformation.of(new TypeHint<InputTuple>() {}), env.getConfig()),
                properties);
        firstStream.addSink(leftProducer);
        if (options.hasSecondStream()) {
            DataStream<InputTuple> secondStream = streamFactory
                    .createDataStream(options.getSecondStream(), options.getDelay(), options.getDuration(),
                            options.getRate(), options.getMinio(), LOG, options.getDimensions(), options.getEmbeddingsFile(),
                            options.getDataset(), options.getSleepsPerSecond(), options.getSleepTime() ,options.getSeed());
            FlinkKafkaProducer<InputTuple> rightProducer = new FlinkKafkaProducer<>(
                    rightOutputTopic,
                    new TypeInformationSerializationSchema<>(TypeInformation.of(new TypeHint<InputTuple>() {}), env.getConfig()),
                    properties);
            secondStream.addSink(rightProducer);
        }
        LOG.info(env.getExecutionPlan());

        // Execute
        JobExecutionResult result = env.execute("generator");
//        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute");
    }
}
