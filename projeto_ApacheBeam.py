import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
  'project': 'gcp-dataflow-beam',
  'runner': 'DataflowRunner',
  'region': 'southamerica-east1',
  'staging_location':'gs://curso-apache-beam1234/temp',
  'temp_location':'gs://curso-apache-beam1234/temp',
  'template_location':'gs://curso-apache-beam1234/template/batch_job_df_gcs_bq',
  'save_main_session': True }
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

class filtro(beam.DoFn):
  def process(self,record):
    if int(record[8])> 0:
      return [record]
    
def criar_dict_nivel1(record):
  dict_ = {}
  dict_['airport'] = record[0]
  dict_['lista']= record[1]
  return(dict_)

def desaninhar_dict(record):
  def expand(key,value):
      if isinstance(value,dict):
        return[ (key+ '_' + k, v) for k,v in desaninhar_dict(value).items()]
      else:
        return[(key,value)]
  items = [item for k,v in record.items() for item in expand(k,v)]
  return dict(items)

def criar_dict_nivel0(record):
  dict_ = {}
  dict_['airport'] = record['airport']
  dict_['lista_Qtd_Atrasos'] = record['lista_Qtd_Atrasos'][0]
  dict_['lista_Tempo_Atrasos'] = record['lista_Tempo_Atrasos'][0]
  return(dict_)

table_schema = 'airport:STRING, lista_Qtd_Atrasos:INTEGER, lista_Tempo_Atrasos:INTEGER'
tabela = 'gcp-dataflow-beam.curso_dataflow.curso_dataflow_voos_atraso'

Tempo_Atrasos = (
p1
  | "Importar Dados Atraso" >> beam.io.ReadFromText(r"gs://curso-apache-beam1234/entrada/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Virgulas Atraso" >> beam.Map(lambda line: line.split(","))
  | "Pegar Voos com atraso" >> beam.ParDo(filtro())
  | "Criar par Atraso" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Somar por key Atraso" >> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
p1
  | "Importar Dados" >> beam.io.ReadFromText(r"gs://curso-apache-beam1234/entrada/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Virgulas" >> beam.Map(lambda line: line.split(","))
  | "Pegar Voos com atraso qtd" >> beam.ParDo(filtro())
  | "Criar par" >> beam.Map(lambda record: (record[4], int(record[8])))
  | "Somar por key" >> beam.combiners.Count.PerKey()
)
tabela_atrasos = (
    {'Qtd_atrasos':Qtd_Atrasos,'Tempo_atrasos':Tempo_Atrasos}
    |"Group By" >> beam.CoGroupByKey()
    |"Criando dict nivel1" >> beam.Map(lambda record: criar_dict_nivel1(record))
    |"Desaninhando dict" >> beam.Map(lambda record: desaninhar_dict(record))
    |"Criando dict nivel0" >> beam.Map(lambda record: criar_dict_nivel0(record))
    |"Saida GCP" >> beam.io.WriteToBigQuery(tabela,
                                            schema=table_schema,
                                            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                            custom_gcs_temp_location= 'gs://curso-apache-beam1234/temp')
)

p1.run()