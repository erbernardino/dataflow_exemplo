package me.emersonrocco.exemplos;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

    /**
     * This is a pipeline that denormalizes exported data from the dataset to
     * create a flattened, denormalized Big Query table
     * <p>
     * In addition to standard Pipeline parameters, this main program takes the following additional parameters:
     * --bigQueryTablename= <project>:<dataset>.<tablename>
     * --loadingBucketURL=gs://<bucketname>
     * <p>
     * An example of how to run this pipeline:
     * mvn compile exec:java \
     * -Dexec.mainClass=BQETLSimple \
     * -Dexec.args="--project=jlb-onboarding \
     * --loadingBucketURL=gs://mb-data \
     * --stagingLocation=gs://mb-data \
     * --runner=BlockingDataflowPipelineRunner \
     * --numWorkers=185 \
     * --maxNumWorkers=500 \
     * --bigQueryTablename=example_project:example_dataset.example_table \
     * --diskSizeGb=1000 \
     * --workerMachineType=n1-standard-1"
     */
    public class EtlEC {
        private static final Logger logger = LoggerFactory.getLogger(EtlEC.class);

        public static void main(String[] args) {
            PipelineOptionsFactory.register(Options.class);

            /*
             * get the custom options
             */
            Options ETLOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
            Pipeline p = Pipeline.create(ETLOptions);


            PCollection<KV<Long, DataObject>> pessoa = DataTransform.loadTable(p, "pessoa", "id");

            PCollection<KV<Long, DataObject>> score1 = DataTransform.loadTable(p, "score1", "id");



            /*
             * perform inner joins
             */

            PCollection<DataObject> pessoaScore1 = DataTransform.innerJoin("pessoa com score1",
                    pessoa, score1);



            TableSchema bqTableSchema = bqSchema();

            PCollection<TableRow> tableRows = DataTransform.transformToTableRows(pessoaScore1, bqTableSchema);

            tableRows.apply(BigQueryIO.Write
                    .named("Write")
                    .to(ETLOptions.getBigQueryTablename())
                    .withSchema(bqTableSchema)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
            //[END bigQueryWrite]
            p.run();
        }


        private static TableSchema bqSchema() {
            FieldSchemaListBuilder fieldSchemaListBuilder = new FieldSchemaListBuilder();

            fieldSchemaListBuilder.intField("pessoa_id")
                    .stringField("pessoa_nome")
                    .stringField("pessoa_doc")
                    .intField("pessoa_score")
                    .intField("score1_score1");
            return fieldSchemaListBuilder.schema();
        }

    }