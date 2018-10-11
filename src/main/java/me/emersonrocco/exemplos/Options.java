package me.emersonrocco.exemplos;

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

public interface Options extends PipelineOptions  {

    @Description("Localizacao Bucket JSONs.")
    @Default.String("gs://dataflow_lpa_sp_2/staging")
    String getLoadingBucketURL();
    void setLoadingBucketURL(String loadingBucketURL);

    @Description("Nome da tabela Big Query ")
    @Default.String("dadosExportados")
    String getBigQueryTablename();
    void setBigQueryTablename(String bigQueryTablename);

    @Description("Opcao de Overwrite tabela BigQuery ")
    @Default.Boolean(true)
    Boolean getOverwriteBigQueryTable();
    void setOverwriteBigQueryTable(Boolean overwriteBigQueryTable);



}
