import sqlite3
from db_utils import ros_time_to_sqlite_time, sqlite_time_to_ros_time, DESIGNATION_DICT
from esiaf_ros.msg import SSLInfo, SSLDir, SpeechInfo, SpeechHypothesis, RecordingTimeStamps


def get_basic_results(type_name, start_time, finish_time, path):

    if type_name not in [DESIGNATION_DICT[x][0] for x in DESIGNATION_DICT if DESIGNATION_DICT[x][0] not in ['VAD', 'SSL', 'SpeechRec']]:
        raise Exception('Type "' + type_name + '" not supported to retrieve!')
    start_time_string = ros_time_to_sqlite_time(start_time)
    finish_time_string = ros_time_to_sqlite_time(finish_time)

    # query all elements of type in question

    sql_query = """
    SELECT {type}, probability, time_from, time_to FROM {type}
    WHERE 
      time_from BETWEEN {time_from} AND {time_to}
      OR
      time_to BETWEEN {time_from} AND {time_to};
    """.format(type=type_name, time_from=start_time_string, time_to=finish_time_string)

    connection = sqlite3.connect(path)
    cursor = connection.cursor()

    cursor.execute(sql_query)
    result_raw = cursor.fetchall()

    connection.commit()
    connection.close()

    # get class of type
    type_class = None
    for designation in DESIGNATION_DICT:
        if DESIGNATION_DICT[designation][0] == type_name:
            type_class = DESIGNATION_DICT[designation][1]

    # iterate over all retrieved instances
    ros_type_list = []
    for instance in result_raw:
        # create new type and fill it with raw result
        type_instance = type_class()
        setattr(type_instance, type_name, instance[0])
        type_instance.probability = instance[1]
        type_instance.time_from = sqlite_time_to_ros_time(instance[2])
        type_instance.time_to = sqlite_time_to_ros_time(instance[3])
        ros_type_list.append(type_instance)

    return ros_type_list


def get_ssl_results(start_time, finish_time, path):
    start_time_string = ros_time_to_sqlite_time(start_time)
    finish_time_string = ros_time_to_sqlite_time(finish_time)

    # query all elements of type in question
    sql_query = """
    SELECT ssl_key, time_from, time_to, sourceId, angleVertical, angleHorizontal, probability FROM 
    ssl 
    LEFT JOIN ssl_combo ON ssl.ssl_key = ssl_combo.ssl_key
    LEFT JOIN ssl_dir ON ssl_dir.dir_key = ssl_combo.dir_key
    WHERE 
      time_from BETWEEN {time_from} AND {time_to}
      OR
      time_to BETWEEN {time_from} AND {time_to};
    """.format(time_from=start_time_string, time_to=finish_time_string)

    connection = sqlite3.connect(path)
    cursor = connection.cursor()

    cursor.execute(sql_query)
    result_raw = cursor.fetchall()

    connection.commit()
    connection.close()

    # recreate ros type instances
    ssl_results = {}
    for entry in result_raw:
        # add sslInfo if key not yet in dict
        if entry[0] not in ssl_results:
            ssl_results[entry[0]] = SSLInfo()
            time_stamp = RecordingTimeStamps()
            time_stamp.start = sqlite_time_to_ros_time(entry[1])
            time_stamp.finish = sqlite_time_to_ros_time(entry[2])
            ssl_results[entry[0]].duration = time_stamp

        # add sslDir info to sslInfo
        sslDir = SSLDir()
        sslDir.sourceId = entry[3]
        sslDir.angleVertical = entry[4]
        sslDir.angleHorizontal = entry[5]
        sslDir.probability = entry[6]
        ssl_results[entry[0]].directions.append(sslDir)

    return [ssl_results[x] for x in ssl_results]
