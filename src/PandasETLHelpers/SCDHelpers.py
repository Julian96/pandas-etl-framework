from src.PandasETLHelpers.MetaColumnHelpers import *


'''#########################################################
# create_empty_hist_dataframe
# Input: df: Dataframe, dessen Schema umd SCD Typ 2 Historisierungsspalten erweitert werden soll
# Erzeugt ein leeres Dataframe mit dem Schema von df, das erweitert wird um die Spalten META_COLUMNS[COL_VALID_FROM] und META_COLUMNS[COL_VALID_TO] 
# Output: Dataframe mit zusätzlichen Spalten META_COLUMNS[COL_VALID_FROM] und META_COLUMNS[COL_VALID_TO]
#########################################################
def create_empty_hist_dataframe(df: pd.DataFrame) -> pd.DataFrame:
  res = pd.DataFrame(columns=df.columns)
  res[META_COLUMNS[COL_VALID_FROM]] = pd.to_datetime('')
  res[META_COLUMNS[COL_VALID_TO]] = pd.to_datetime('')
  res = create_dataframe_with_schema(df.schema) \
        .withColumn(META_COLUMNS[COL_VALID_FROM],psf.lit(None).cast(pst.DateType())) \
        .withColumn(META_COLUMNS[COL_VALID_TO],psf.lit(None).cast(pst.DateType()))
  
  return res

#########################################################
# create_dataframe_with_schema
# Input: schema: Schema des leeren Dataframes, das erzeugt werden soll
# Erzeugt ein leeres Dataframe mit gegebenem Schema
# Output: Dataframe mit gegebenem Schema
#########################################################
def create_dataframe_with_schema(schema: pst.StructType)  -> pd.DataFrame:
    emptyRDD = spark.sparkContext.emptyRDD()
    res = spark.createDataFrame(emptyRDD, schema)
    
    return res

  

#########################################################
# prepare_schema
# Input: df: Dataframe, dessen Schema angepasst werden soll
#        new_schema: neues Schema, das auf df angewandt werden soll
#        default_values: Dictionary mit Defaultwerten für neue Spalten. Z.B. {"new_column_int" : 5, "new_column_str" : "default_value"}
#        remove_columns: Boolean, ob Spalten aus df entfernt werden sollen, die nicht in new_schema definiert sind
# Wendet ein gegebenes Schema auf ein Dataframe an. Neue Spalten werden hinzugefügt und mit den Defaultwerten aus 
# default_values ergänzt. Entfernt ggfs. überschüssige Spalten aus df, sofern remove_columns True ist
# Output: Dataframe mit neuem Schema
#########################################################
def prepare_schema(df: pd.DataFrame, new_schema: pst.StructType, default_values: dict = None, remove_columns: bool = False) -> psd.DataFrame:
    res = df
    old_fields = set((f.name, f.dataType, f.nullable) for f in df.schema)
    new_fields = set((f.name, f.dataType, f.nullable) for f in new_schema)

    # Add missing fields first
    for col_name, col_type, col_nullable in new_fields.difference(old_fields):
        default_value = None
        if default_values is not None:
            if col_name in default_values.keys():
                default_value = default_values[col_name]
        res = res.withColumn(col_name, psf.lit(default_value).cast(col_type))

    if remove_columns:
        for col_name, col_type, col_nullable in old_fields.difference(new_fields):
            res = res.drop(col_name)

    return res


#########################################################
# merge_cdc
#########################################################
def merge_cdc(current_df: psd.DataFrame, new_df: psd.DataFrame, key_columns: list, deleted_df: psd.DataFrame = None) -> psd.DataFrame:
    res = current_df
    if deleted_df is not None:
        res = res.join(deleted_df, key_columns, "leftanti").select(res.columns)
    res = res.join(new_df, key_columns, "leftanti").select(res.columns)
    res = res.unionAll(new_df)
    return res
  

#########################################################
# get_valid_from_date
# Input: valid_from_mode: Integer, der den valid_from_mode bestimmt
#                         VALID_FROM_MODE_LOWER_BOUND, VALID_FROM_MODE_LOAD_DATE, VALID_FROM_MODE_CUSTOM
#        valid_from_date: Datum, das bei VALID_FROM_MODE_CUSTOM als valid_from gesetzt werden soll 
#        currents: Dictionary mit Zeitwerten. Muss je nach valid_from_mode die Werte CURRENT_RUN_TS, CURRENT_RUN_DAY und/oder CURRENT_RUN_ID beinhalten.
# Funktion, die je nach Mode das valid_from date zurückgibt.
# VALID_FROM_MODE_LOWER_BOUND: setzt valid_from auf SCD2_LOWER_BOUND
# VALID_FROM_MODE_LOAD_DATE: setzt valid_from auf currents[CURRENT_RUN_DAY]. currents muss dafür gesetzt sein
# VALID_FROM_MODE_CUSTOM: setzt valid_from auf valid_from_date. valid_from_date muss dafür gesetzt sein
# Output: String mit jeweiligem valid_from Wert
#########################################################
def get_valid_from_date(valid_from_mode: int, valid_from_date: str = None, currents: dict = None):
  valid_from = None
  if valid_from_mode == VALID_FROM_MODE_LOWER_BOUND:
    valid_from = SCD2_LOWER_BOUND
  elif valid_from_mode == VALID_FROM_MODE_LOAD_DATE:
    if currents is None:
      print("If valid_from_mode = VALID_FROM_MODE_LOAD_DATE the currents parameter has to be set. Exiting function")
      return
    else:
      valid_from = currents[CURRENT_RUN_DAY]
  elif valid_from_mode == VALID_FROM_MODE_CUSTOM:
    if valid_from_date is None:
      print("If valid_from_mode = VALID_FROM_MODE_CUSTOM the valid_from_date parameter has to be set. Exiting function")
      return
    else:
      valid_from = valid_from_date
  else:
    print("Error. Mode must be one in VALID_FROM_MODE_LOWER_BOUND, VALID_FROM_MODE_LOAD_DATE, VALID_FROM_MODE_CUSTOM. Defaulting to VALID_FROM_MODE_LOWER_BOUND")
    valid_from = SCD2_LOWER_BOUND
  print(valid_from)
  return valid_from


#########################################################
# merge_scd2
# Input: spark: SparkSession, um SQL-Engine zu nutzen
#        current_df: Dataframe, das den aktuellen Datenbestand beinhaltet. Muss Hashes und COL_VALID_TO + COL_VALID_FROM als Spalten haben
#        new_df: Dataframe, das die neugeladenen Daten beinhaltet. Muss META_COLUMNS als Spalten haben
#        currents: Dictionary mit Zeitwerten. Muss je nach valid_from_mode die Werte CURRENT_RUN_TS, CURRENT_RUN_DAY und/oder CURRENT_RUN_ID beinhalten.
#        valid_from_mode: Integer, der den valid_from_mode bestimmt (Welches valid_from Datum an neue Datensätze geschrieben wird). 
#                         VALID_FROM_MODE_LOWER_BOUND, VALID_FROM_MODE_LOAD_DATE, VALID_FROM_MODE_CUSTOM
#        valid_from_date: Datum, das bei VALID_FROM_MODE_CUSTOM als valid_from gesetzt werden soll
# Mergt die Dataframes current_df und new_df zusammen und historisiert die Daten nach SCD Typ 2. 
# Erzeugt 5 Dataframes, die über union zusammengefüht werden:
#   current_only_df: Datensätze, die nur current_df sind oder bereits vollständig historisiert
#   new_only_df: Datensätze, die nur in new_df sind. valid_from wird hier je nach valid_from_mode gesetzt
#   unchanged_current_df: Datensätze, die unverändert und aktiv in new_df und current_df sind
#   changed_current_df: Datensätze aus current_df, die aktiv sind und verändert in new_df vorkommen. Sind jetzt historisiert und valid_to wird gesetzt
#   changed_new_df: Datensätze aus new_df, die die Veränderung beinhalten. Neue aktive Datensätze mit valid_from ab CURRENT_RUN_DAY
# Output: Zusammengeführtes Dataframe der 5 oben genannten
#########################################################
def merge_scd2(current_df: pd.DataFrame, new_df: pd.DataFrame, currents: dict, valid_from_mode: int, valid_from_date: str = None) -> pd.DataFrame:
    current_table_name = 'current_' + currents[CURRENT_RUN_ID]
    new_table_name = 'new_' + currents[CURRENT_RUN_ID]
    
    #current_df = spark.read.parquet(os.path.join(dataset_base_path,active_suffix))
    current_df.registerTempTable(current_table_name)
    
    new_df.registerTempTable(new_table_name)

    print('current only')
    current_only_df = spark.sql('select c.* from ' + current_table_name + ' c '
                                + ' left outer join ' + new_table_name + ' n '
                                + ' on (c.' + META_COLUMNS[COL_KEY_HASH] + ' = n.' + META_COLUMNS[
                                    COL_KEY_HASH] + ')'
                                + ' where n.' + META_COLUMNS[COL_KEY_HASH] + ' is null '
                                + ' or c.' + META_COLUMNS[
                                    COL_VALID_TO] + ' <> TO_DATE(CAST(UNIX_TIMESTAMP(\'' + SCD2_UPPER_BOUND + '\', \'' + SPARK_DAY_FORMAT + '\') AS TIMESTAMP)) ')

    current_only_df.show(truncate=False)

    print('new only')
    # enthält die komplett neuen eingefügten aktiven Datensätze --> Muss nach aktiv geschrieben werden
    valid_from = get_valid_from_date(valid_from_mode, valid_from_date, currents)
    
    
    new_only_df = spark.sql('select n.* from ' + new_table_name + ' n '
                            + ' where n.' + META_COLUMNS[COL_KEY_HASH] + ' not in '
                            + ' (select ' + META_COLUMNS[COL_KEY_HASH] + ' from ' + current_table_name + ' )')
    new_only_df = new_only_df.withColumn(META_COLUMNS[COL_VALID_FROM],
                                         to_date(psf.date_format(psf.lit(valid_from), SPARK_DAY_FORMAT)))
    new_only_df = new_only_df.withColumn(META_COLUMNS[COL_VALID_TO],
                                         to_date(psf.date_format(psf.lit(SCD2_UPPER_BOUND), SPARK_DAY_FORMAT)))
    new_only_df.show(truncate=False)

    print('unchanged current')
    # enthält die ungeänderten aktiven Datensätze --> Muss nach aktiv geschrieben werden
    unchanged_current_df = spark.sql('select c.* from ' + current_table_name + ' c '
                                     + ' inner join ' + new_table_name + ' n '
                                     + ' on ( c.' + META_COLUMNS[COL_KEY_HASH] + ' = n.' + META_COLUMNS[
                                         COL_KEY_HASH]
                                     + '  and c.' + META_COLUMNS[COL_RECORD_HASH] + ' = n.' + META_COLUMNS[
                                         COL_RECORD_HASH] + ')'
                                     + ' and c.' + META_COLUMNS[
                                         COL_VALID_TO] + ' = TO_DATE(CAST(UNIX_TIMESTAMP(\'' + SCD2_UPPER_BOUND + '\', \'' + SPARK_DAY_FORMAT + '\') AS TIMESTAMP))')
    unchanged_current_df.show(truncate=False)

    print('changed current')
    # enthält die neuen historisierten Datensätze mit neuem VALID_TO date --> Muss nach hist angefügt werden
    changed_current_df = spark.sql('select c.* from ' + current_table_name + ' c '
                                   + ' inner join ' + new_table_name + ' n '
                                   + ' on ( c.' + META_COLUMNS[COL_KEY_HASH] + ' = n.' + META_COLUMNS[
                                       COL_KEY_HASH]
                                   + '  and c.' + META_COLUMNS[COL_RECORD_HASH] + ' <> n.' + META_COLUMNS[
                                       COL_RECORD_HASH]
                                   + '  and c.' + META_COLUMNS[
                                       COL_VALID_TO] + ' = TO_DATE(CAST(UNIX_TIMESTAMP(\'' + SCD2_UPPER_BOUND + '\', \'' + SPARK_DAY_FORMAT + '\') AS TIMESTAMP)))')
    changed_current_df = changed_current_df.withColumn(META_COLUMNS[COL_UPDATE_RUN_TS],
                                                       psf.unix_timestamp(psf.lit(
                                                           currents[CURRENT_RUN_TS]),
                                                                          SPARK_TS_FORMAT).cast("timestamp"))
    changed_current_df = changed_current_df.withColumn(META_COLUMNS[COL_UPDATE_RUN_ID],
                                                       psf.lit(currents[CURRENT_RUN_ID]))
    changed_current_df = changed_current_df.withColumn(META_COLUMNS[COL_VALID_TO],
                                                       psf.date_sub(psf.date_format(psf.lit(
                                                           currents[CURRENT_RUN_DAY]),
                                                                                    SPARK_DAY_FORMAT), 1))
    changed_current_df.show(truncate=False)

    print('changed new')
    # Enthält die neuen aktiven Datensätze mit neuem VALID_FROM date  --> Muss nach aktiv geschrieben werden
    changed_new_df = spark.sql('select n.* from ' + new_table_name + ' n '
                               + ' inner join ' + current_table_name + ' c '
                               + ' on ( c.' + META_COLUMNS[COL_KEY_HASH] + ' = n.' + META_COLUMNS[
                                   COL_KEY_HASH]
                               + '  and c.' + META_COLUMNS[COL_RECORD_HASH] + ' <> n.' + META_COLUMNS[
                                   COL_RECORD_HASH]
                               + '  and c.' + META_COLUMNS[
                                       COL_VALID_TO] + ' = TO_DATE(CAST(UNIX_TIMESTAMP(\'' + SCD2_UPPER_BOUND + '\', \'' + SPARK_DAY_FORMAT + '\') AS TIMESTAMP)))')
    changed_new_df = changed_new_df.withColumn(META_COLUMNS[COL_VALID_FROM],
                                               to_date(psf.date_format(
                                                   psf.lit(currents[CURRENT_RUN_DAY]),
                                                   SPARK_DAY_FORMAT)))
    changed_new_df = changed_new_df.withColumn(META_COLUMNS[COL_VALID_TO],
                                               to_date(psf.date_format(psf.lit(SCD2_UPPER_BOUND), SPARK_DAY_FORMAT)))
    changed_new_df.show(truncate=False)
    
    res = current_only_df.unionByName(new_only_df).unionByName(unchanged_current_df).unionByName(changed_current_df).unionByName(
        changed_new_df)
          
    spark.catalog.dropTempView(current_table_name)
    spark.catalog.dropTempView(new_table_name)
    return res


  
#########################################################
# get_deletes_by_column
# Input: df: Dataframe aus welchem gelöschte Datensätze identifiziert werden sollen
#        del_col_name: String mit Spaltenname, in welchem nach del_col_value gesucht werden soll
#        del_col_value: Wert, der bestimmt, welche Datensätze als gelöscht identifiziert werden sollen
# Sucht im Dataframe df nach dem Wert del_col_value in der Spalte del_col_name und gibt die Key-Hashes der
# passenden Datensätze als Liste zurück
# Output: Liste mit Key-Hashes
#########################################################
def get_deletes_by_column(df: pd.DataFrame, del_col_name: str, del_col_value) -> list:
  deleted_key_hash_list = df.filter(df[del_col_name] == del_col_value).select(META_COLUMNS[COL_KEY_HASH]).rdd.flatMap(lambda x: x).collect()
  return deleted_key_hash_list


#########################################################
# get_deleted_by_full_load
# Input: current_df: Dataframe, das den aktuellen Datenbestand beinhaltet. Muss META_COLUMNS als Spalten haben
#        new_df: Dataframe, das die neugeladenen Daten beinhaltet. Muss META_COLUMNS als Spalten haben
# Identifiziert die Datensätze, die in current_df sind, aber nicht in new_df. Gibt die Key-Hashes der Datensätze als
# Liste zurück. Nur sinnvoll, wenn new_df und current_df ein Full Load sind.
# Output: Liste mit Key-Hashes
#########################################################
def get_deleted_by_full_load(current_df: pd.DataFrame, new_df: pd.DataFrame):
    spark = SparkSession.builder.getOrCreate()
    current_table_name = 'current_df'
    new_table_name = 'new_df'
    
    #current_df = spark.read.parquet(os.path.join(dataset_base_path,active_suffix))
    current_df.registerTempTable(current_table_name)
    
    new_df.registerTempTable(new_table_name)

    #print('current only')
    current_only_df = spark.sql('select c.* from ' + current_table_name + ' c '
                                + ' left outer join ' + new_table_name + ' n '
                                + ' on (c.' + META_COLUMNS[COL_KEY_HASH] + ' = n.' + META_COLUMNS[
                                    COL_KEY_HASH] + ')'
                                + ' where n.' + META_COLUMNS[COL_KEY_HASH] + ' is null')
    spark.catalog.dropTempView(current_table_name)
    spark.catalog.dropTempView(new_table_name)
    
    deleted_key_hash_list = current_only_df.select(META_COLUMNS[COL_KEY_HASH]).rdd.flatMap(lambda x: x).collect()
    return deleted_key_hash_list'''
  
  
#########################################################
# read_df
# Input: path: String mit Pfad, von welchem Daten gelesen werden sollen
# Liest die Daten von path als parquet. Bei Fehler (z.B. keine Daten liegen am gegebenen Pfad)
# wird None zurückgegeben
# Output: Dataframe oder None
#########################################################
def read_parquet_df(path: str):
  try:
    df = pd.read_parquet(path)
    return df
  except:
    return None

  
'''#########################################################
# historize_dataset
# Input: spark: SparkSession, um SQL-Engine zu nutzen
#        current_df: Dataframe, das den aktuellen Datenbestand beinhaltet. Muss Hashes und COL_VALID_TO + COL_VALID_FROM als Spalten haben
#        new_df: Dataframe, das die neugeladenen Daten beinhaltet. Muss META_COLUMNS als Spalten haben
#        currents: Dictionary mit Zeitwerten. Muss je nach valid_from_mode die Werte CURRENT_RUN_TS, CURRENT_RUN_DAY und/oder CURRENT_RUN_ID beinhalten.
#        valid_from_mode: Integer, der den valid_from_mode bestimmt (Welches valid_from Datum an neue Datensätze geschrieben wird). 
#                         VALID_FROM_MODE_LOWER_BOUND, VALID_FROM_MODE_LOAD_DATE, VALID_FROM_MODE_CUSTOM
#        valid_from_date: Datum, das bei VALID_FROM_MODE_CUSTOM als valid_from gesetzt werden soll
# Wrapperfunktion für merge_scd2. Für den Fall, dass noch keine aktuellen Daten vorliegen wird ein leeren Dataframe für die Historisierung
# erzeugt, das die nötigen Spalten beinhaltet.
# Output: Dataframe nach SCD Typ 2 historisiert
#########################################################
def historize_dataset(new_df: pd.DataFrame, current_df: pd.DataFrame ,currents: dict, valid_from_mode: int, valid_from_date: str = None) -> pd.DataFrame:
  if current_df == None:
    current_df = create_empty_hist_dataframe(new_df)
  res = merge_scd2(current_df, new_df, currents, valid_from_mode, valid_from_date)
  return res'''


#########################################################
# split_merged_dataset
# Input: df: Dataframe, das aufgeteilt werden soll
# Teilt ein Dataframe in aktive und historisierte Datensätze auf. Nur aktive sind relevant für die weitere Historisierung
# Werden aufgeteilt nach SCD2_UPPER_BOUND
# Output: 2 Dataframes, je mit aktiven und abgeschlossenen Datensätzen
#########################################################
def split_merged_dataset(df: pd.DataFrame):
  hist = df[df[META_COLUMNS[COL_VALID_TO]] > SCD2_UPPER_BOUND]
  active = df[df[META_COLUMNS[COL_VALID_TO]] == SCD2_UPPER_BOUND]
  hist.show()
  active.show()
  return hist, active