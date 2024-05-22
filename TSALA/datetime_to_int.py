import sqlite3
import pandas as pd

def split_datetime_and_save(db_path, source_table, new_table):
    conn = sqlite3.connect(db_path)

    query = f"SELECT * FROM {source_table}"
    df = pd.read_sql(query, conn)

    # drop data that has no datetime
    df = df[df['startTime'] != -1]

    # drop data that has no stripesize
    df = df[df['stripeSize'] != -1]

    df['startTime'] = pd.to_datetime(df['startTime'])
    df['endTime'] = pd.to_datetime(df['endTime'])

    reference_time = df['startTime'].iloc[0]

     # startTime
    start_idx = df.columns.get_loc('startTime') + 1
    df.insert(start_idx, 'relativeStartTime', (df['startTime'] - reference_time).dt.total_seconds())
    df.insert(start_idx + 1, 'startYear', df['startTime'].dt.year)
    df.insert(start_idx + 2, 'startMonth', df['startTime'].dt.month)
    df.insert(start_idx + 3, 'startDay', df['startTime'].dt.day)
    df.insert(start_idx + 4, 'startHour', df['startTime'].dt.hour)
    df.insert(start_idx + 5, 'startMinute', df['startTime'].dt.minute)
    df.insert(start_idx + 6, 'startSecond', df['startTime'].dt.second)
                                                                         
    # endTime
    end_idx = df.columns.get_loc('endTime') + 1 
    df.insert(end_idx, 'relativeEndTime', (df['endTime'] - reference_time).dt.total_seconds())
    df.insert(end_idx + 1, 'endYear', df['endTime'].dt.year)
    df.insert(end_idx + 2, 'endMonth', df['endTime'].dt.month)
    df.insert(end_idx + 3, 'endDay', df['endTime'].dt.day)
    df.insert(end_idx + 4, 'endHour', df['endTime'].dt.hour)
    df.insert(end_idx + 5, 'endMinute', df['endTime'].dt.minute)
    df.insert(end_idx + 6, 'endSecond', df['endTime'].dt.second)

    # # delete useless columns
    # columns_to_drop = ['ioStartTime', 'numCPU', 'numNode', 'numOST', 'knl', 'mdsOPSMin', 'totalFileSTDIO', 'readRateTotal',
    #                 'writeBytesMPIIO', 'writeTimeMPIIO', 'writeRateMPIIO', 'readBytesMPIIO', 'writeBytesMPIIO', 'writeTimesSTDIO',
    #                 'writeRateSTDIO', 'readBytesSTDIO', 'readTimeSTDIO', 'ostlist']
    # df.drop(columns=columns_to_drop, inplace=True)
    
    df.to_sql(new_table, conn, if_exists='replace', index=False)

    conn.close()

source_db_path = './total_all.db'
source_table_name = 'total_sorted'
new_table_name = "total_sorted_add_time"
split_datetime_and_save(source_db_path, source_table_name, new_table_name)
