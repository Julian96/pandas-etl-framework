import datetime
import pandas as pd
from hashlib import md5

#########################################################
# meta columns constants
#########################################################
COL_KEY_HASH = 1
COL_RECORD_HASH = 2
COL_VALID_FROM = 3
COL_VALID_TO = 4
COL_INSERT_RUN_TS = 5
COL_UPDATE_RUN_TS = 6
COL_INSERT_RUN_ID = 7
COL_UPDATE_RUN_ID = 8
COL_SOURCE_FILE_NAME = 9
COL_DELETED = 10

META_COLUMNS = {
    COL_KEY_HASH: 'KEY_HASH',
    COL_RECORD_HASH: 'RECORD_HASH',
    COL_VALID_FROM: 'VALID_FROM',
    COL_VALID_TO: 'VALID_TO',
    COL_INSERT_RUN_TS: 'INSERT_TS',
    COL_UPDATE_RUN_TS: 'UPDATE_TS',
    COL_INSERT_RUN_ID: 'INSERT_RUN_ID',
    COL_UPDATE_RUN_ID: 'UPDATE_RUN_ID',
    COL_SOURCE_FILE_NAME: 'SOURCE_FILE_NAME',
    COL_DELETED: 'DELETED'
}

CURRENT_RUN_ID = 'CURRENT_RUN_ID'
CURRENT_RUN_TS = 'CURRENT_RUN_TS'
CURRENT_RUN_DAY = 'CURRENT_RUN_DAY'

VALID_FROM_MODE_LOWER_BOUND = 1
VALID_FROM_MODE_LOAD_DATE = 2
VALID_FROM_MODE_CUSTOM = 3

VALID_TO_MODE_LOAD_DATE = 1
VALID_TO_MODE_CUSTOM = 2

RUN_ID_FORMAT = '%Y%m%d%H%M%S'

PYTHON_TS_FORMAT = '%Y-%m-%d %H:%M:%S'
SPARK_TS_FORMAT = 'yyyy-MM-dd HH:mm:ss'

PYTHON_DAY_FORMAT = '%Y-%m-%d'
SPARK_DAY_FORMAT = 'yyyy-MM-dd'

SCD2_LOWER_BOUND = '1900-01-01'
SCD2_UPPER_BOUND = '9999-12-31'


#########################################################
# create_currents
# Input: load_ts: String mit dem Format PYTHON_TS_FORMAT
# CURRENT_RUN_ID: load_ts im Format RUN_ID_FORMAT (String)
# CURRENT_RUN_DAY: load_ts auf Granularität Tag (String)
# CURRENT_RUN_TS: load_ts als datetime (Datetime)
# Output: Dictionary mit oben genannten Werten
#########################################################
def create_currents(load_ts: str = None):
  if load_ts is not None:
    load_ts_as_datetime = datetime.datetime.strptime(load_ts, PYTHON_TS_FORMAT)
    res = {
      CURRENT_RUN_ID   : load_ts_as_datetime.strftime(RUN_ID_FORMAT),
      CURRENT_RUN_DAY : load_ts_as_datetime.date().strftime(PYTHON_TS_FORMAT),
      CURRENT_RUN_TS   : load_ts
    }
  else:
    now = datetime.datetime.now()
    res = {
      CURRENT_RUN_ID : now.strftime(RUN_ID_FORMAT),
      CURRENT_RUN_DAY : now.date().strftime(PYTHON_TS_FORMAT),
      CURRENT_RUN_TS: now.strftime(PYTHON_TS_FORMAT)
    }
  return res


#########################################################
# add_hash_columns
# Input: df: Dataframe dessen Spalten gehasht werden sollen
#        columns: Liste mit Spalten die gehasht werden sollen
#        hash_column_name: Name der neu angefügten Hash-Spalte
# Fügt die Spalte hash_column_name an das Dataframe df an, die 
# die Spalten in der Liste columns nach md5 hasht und die Werte
# mit dem Trennzeichen #? konkateniert
# Output: Dataframe mit der neuen Spalte
#########################################################
def add_hash_column(df: pd.DataFrame, columns: list, hash_column_name: str) -> pd.DataFrame:
    res = df.copy()
    res[hash_column_name] = res[columns[0]].astype(str)
    for column in columns[1:]:
        res[hash_column_name] = res[hash_column_name] + "#?" + res[column].astype(str)
    res.loc[:, hash_column_name] = res.loc[:, hash_column_name].apply(lambda x: md5(x.encode("utf8")).hexdigest())
    return res

  
#########################################################
# add_key_hash
# Input: df: Dataframe, dessen Keys gehasht werden sollen
#        key_columns: Liste mit den Spaltennamen des Keys
# Wrapperfunktion für add_hash_column. Gibt die Inputparameter
# weiter und setzt als hash_column_name META_COLUMNS[COL_KEY_HASH] ein
# Output: Dataframe mit META_COLUMNS[COL_KEY_HASH] Spalte
#########################################################
def add_key_hash(df: pd.DataFrame, key_columns: list) -> pd.DataFrame:
    print("KEY_HASH Columns: " + str(key_columns))
    return add_hash_column(df, key_columns, META_COLUMNS[COL_KEY_HASH])


#########################################################
# add_record_hash
# Input: df: Dataframe, dessen Values gehasht werden sollen
#        exclude_columns: Liste mit Spalten, die nicht im Record gehasht werden sollen
# Wrapperfunktion für add_hash_column. Erzeugt die Liste an Spalten, die gehasht werden sollen.
# Entfernt die Metadaten Spalten aus META_COLUMNS und die Spalten aus exclude_columns.
# Setzt als hash_column_name META_COLUMNS[COL_RECORD_HASH] ein.
# Output: Dataframe mit META_COLUMNS[COL_KEY_HASH] Spalte
#########################################################
def add_record_hash(df: pd.DataFrame, exclude_columns: list = None) -> pd.DataFrame:
    column_filter = list(META_COLUMNS.values())
    if exclude_columns is not None:
        column_filter.extend(exclude_columns)
    filtered_columns = list(filter(lambda x: x not in column_filter, df.columns))
    print("RECORD_HASH Columns: " + str(filtered_columns))
    return add_hash_column(df, filtered_columns, META_COLUMNS[COL_RECORD_HASH])

  
#########################################################
# add_meta_columns
# Input: df: Dataframe, an das die Metadatenspalten angefügt werden sollen
#        currents: Dictionary mit Zeitwerten. Muss die Werte CURRENT_RUN_TS und CURRENT_RUN_ID beinhalten.
#        key_columns: Liste mit den Spaltennamen des Keys
#        record_hash_exclude_columns: Liste mit Spalten, die nicht im Record gehasht werden sollen
# Fügt Metadatenspalten an ein Dataframe an. Diese sind der Key-Hash, der Record-Hash, Insert/Update Timestamp
# und Run-ID, Dateiname, in welchem der Datensatz zu finden ist und das Deleted Flag.
# Output: Dataframe mit angefügten Metadatenspalten
#########################################################
def add_meta_columns(df: pd.DataFrame, currents: map, key_columns: list, record_hash_exclude_columns: list = None) \
        -> pd.DataFrame:
    df1 = add_key_hash(df, key_columns)
    df2 = add_record_hash(df1, record_hash_exclude_columns)
    res = df2
    res[META_COLUMNS[COL_INSERT_RUN_TS]] = pd.to_datetime(currents[CURRENT_RUN_TS])
    res[META_COLUMNS[COL_UPDATE_RUN_TS]] = pd.to_datetime(currents[CURRENT_RUN_TS])
    res[META_COLUMNS[COL_INSERT_RUN_ID]] = currents[CURRENT_RUN_ID]
    res[META_COLUMNS[COL_UPDATE_RUN_ID]] = currents[CURRENT_RUN_ID]
    res[META_COLUMNS[COL_DELETED]] = pd.to_datetime('')

    return res


  
#########################################################
# read_current_hashes
# Input: path: Pfad der ausgelesen werden soll.
# Lädt die Daten am gegebenen Pfad als Dataframe und selektiert nur die Hash-Spalten
# Kommt es zu einem Fehler (z.B. liegen keine Daten vor am Pfad, Key-Spalten fehlen)
# wird None zurückgegeben
# Output: Dataframe nur mit Hash-Spalten oder None
#########################################################
def read_current_hashes(path: str):
  try:
    df = spark.read.parquet(path).select(META_COLUMNS[COL_KEY_HASH],META_COLUMNS[COL_RECORD_HASH])
    return df
  except:
    return None


#########################################################
# get_delta
# Input: current_data: Dataframe, das den aktuellen Datenbestand beinhaltet. Muss META_COLUMNS als Spalten haben
#        new_data: Dataframe, das die neugeladenen Daten beinhaltet. Muss META_COLUMNS als Spalten haben
# Berechnet das Delta (Inserts und Updates) zwischen current_data und new_data. Gibt die Datensätze zurück, die nur
# in new_data zufinden sind (über den Key-Hash), sowie die Datensätze, die den gleichen Key-Hash haben aber unterschiedliche Record-Hashes
# Output: Dataframe das nur Inserts und Updates aus new_data beinhaltet
#########################################################
def get_delta(current_data: pd.DataFrame, new_data: pd.DataFrame):
  delta = new_data.join(current_data,
                  (new_data[META_COLUMNS[COL_KEY_HASH]] == current_data[META_COLUMNS[COL_KEY_HASH]]) & (new_data[META_COLUMNS[COL_RECORD_HASH]] == current_data[META_COLUMNS[COL_RECORD_HASH]]),
                  "leftanti")
  return delta


#########################################################
# get_inserts
# Input: current_data: Dataframe, das den aktuellen Datenbestand beinhaltet. Muss META_COLUMNS als Spalten haben
#        new_data: Dataframe, das die neugeladenen Daten beinhaltet. Muss META_COLUMNS als Spalten haben
# Berechnet die Inserts - Datensätze (Key-Hash), die sind new_data vorliegen, aber nicht in current_data
# Output: Dataframe, das nur Inserts aus new_data beinhaltet
#########################################################
def get_inserts(current_data: pd.DataFrame, new_data: pd.DataFrame):
  inserts = new_data.join(current_data,new_data[META_COLUMNS[COL_KEY_HASH]] == current_data[META_COLUMNS[COL_KEY_HASH]],"leftanti")
  return inserts


#########################################################
# get_updates
# Input: current_data: Dataframe, das den aktuellen Datenbestand beinhaltet. Muss META_COLUMNS als Spalten haben
#        new_data: Dataframe, das die neugeladenen Daten beinhaltet. Muss META_COLUMNS als Spalten haben
# Berechnet die Updates - Datensätze (gleicher Key-Hash aber anderer Record-Hash) aus new_data
# Output: Dataframe, das nur Updates aus new_data beinhaltet
#########################################################
def get_updates(current_data: pd.DataFrame, new_data: pd.DataFrame):
  updates = new_data.join(current_data,
                  (new_data[META_COLUMNS[COL_KEY_HASH]] == current_data[META_COLUMNS[COL_KEY_HASH]]) & (new_data[META_COLUMNS[COL_RECORD_HASH]] != current_data[META_COLUMNS[COL_RECORD_HASH]]))
  return updates