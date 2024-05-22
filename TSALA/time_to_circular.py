import sqlite3
import pandas as pd
import numpy as np
def calculate_trigonometric_features_and_save(db_path, source_table, new_table):

    conn = sqlite3.connect(db_path)

    query = f"SELECT * FROM {source_table}"
    df = pd.read_sql(query, conn)

    startMonthDaySin = np.sin(2 * np.pi * (df['startMonth'] / 12 + df['startDay'] / 365.25))
    startMonthDayCos = np.cos(2 * np.pi * (df['startMonth'] / 12 + df['startDay'] / 365.25))

    endMonthDaySin = np.sin(2 * np.pi * (df['endMonth'] / 12 + df['endDay'] / 365.25))
    endMonthDayCos = np.cos(2 * np.pi * (df['endMonth'] / 12 + df['endDay'] / 365.25))

    startDaytimeSin = np.sin(2 * np.pi * (df['startHour'] / 24 + df['startMinute'] / 1440 + df['startSecond'] / 86400))
    startDaytimeCos = np.cos(2 * np.pi * (df['startHour'] / 24 + df['startMinute'] / 1440 + df['startSecond'] / 86400))

    endDaytimeSin = np.sin(2 * np.pi * (df['endHour'] / 24 + df['endMinute'] / 1440 + df['endSecond'] / 86400))
    endDaytimeCos = np.cos(2 * np.pi * (df['endHour'] / 24 + df['endMinute'] / 1440 + df['endSecond'] / 86400))

     # startTime
    start_idx = df.columns.get_loc('startSecond') + 1
    df.insert(start_idx, 'startMonthDaySin', startMonthDaySin)
    df.insert(start_idx + 1, 'startMonthDayCos', startMonthDayCos)
    df.insert(start_idx + 2, 'startDaytimeSin', startDaytimeSin)
    df.insert(start_idx + 3, 'startDaytimeCos', startDaytimeCos)
                                                                         
    # endTime
    end_idx = df.columns.get_loc('endSecond') + 1 
    df.insert(end_idx, 'endMonthDaySin', endMonthDaySin)
    df.insert(end_idx + 1, 'endMonthDayCos', endMonthDayCos)
    df.insert(end_idx + 2, 'endDaytimeSin', endDaytimeSin)
    df.insert(end_idx + 3, 'endDaytimeCos', endDaytimeCos)

    # delete naive time columns
    columns_to_drop = ['startYear', 'startMonth', 'startDay', 'startHour', 'startMinute', 'startSecond', 
                       'endYear', 'endMonth', 'endDay', 'endHour', 'endMinute', 'endSecond']
    df.drop(columns=columns_to_drop, inplace=True)
    print(len(df.columns))

    df.to_sql(new_table, conn, if_exists='replace', index=False)

    conn.close()

source_db_path = './total_all.db'
source_table_name = "total_sorted_add_time"
new_table_name = "total_include_time"

calculate_trigonometric_features_and_save(source_db_path, source_table_name, new_table_name)