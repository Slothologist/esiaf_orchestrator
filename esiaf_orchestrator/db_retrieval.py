import sqlite3
from db_utils import ros_time_to_sqlite_time, sqlite_time_to_ros_time, integer_to_ros_duration, DESIGNATION_DICT
from esiaf_ros.msg import SSLInfo, SSLDir, SpeechInfo, SpeechHypothesis, RecordingTimeStamps, VADInfo


def get_results(type_name, start_time, finish_time, path):
    type_dict = {'SSL': 'ssl', 'SpeechRec': 'speech', 'VAD': 'vad'}
    db_table_name = type_name
    if type_name in type_dict:
        db_table_name = type_dict[type_name]
    latency_query = """
        SELECT AVG(latency) FROM {type};
        """.format(type=db_table_name)


    connection = sqlite3.connect(path)
    cursor = connection.cursor()

    cursor.execute(latency_query)
    latency = cursor.fetchone()[0] or 1

    connection.commit()
    connection.close()

    latency = integer_to_ros_duration(int(latency))

    if type_name == 'SSL':
        return _get_ssl_results(start_time, finish_time, path), latency
    elif type_name == 'SpeechRec':
        return _get_speech_rec_results(start_time, finish_time, path), latency
    elif type_name == 'VAD':
        return _get_vad_results(start_time, finish_time, path), latency
    else:
        return _get_basic_results(type_name, start_time, finish_time, path), latency


def _get_basic_results(type_name, start_time, finish_time, path):

    if type_name not in [DESIGNATION_DICT[x][0] for x in DESIGNATION_DICT if DESIGNATION_DICT[x][0] not in ['VAD', 'SSL', 'SpeechRec']]:
        raise Exception('Type "' + type_name + '" not supported to retrieve!')
    start_time_string = ros_time_to_sqlite_time(start_time)
    finish_time_string = ros_time_to_sqlite_time(finish_time)

    # query all elements of type in question

    sql_query = """
    SELECT {type}, probability, time_from, time_to FROM {type}
    WHERE 
      time_from BETWEEN "{time_from}" AND "{time_to}"
      OR
      time_to BETWEEN "{time_from}" AND "{time_to}";
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

    # edge case: no entry yet
    if len(result_raw) == 1 and not [x for x in result_raw[0] if x is not None]:
        return []

    # iterate over all retrieved instances
    ros_type_list = []
    for instance in result_raw:
        # create new type and fill it with raw result
        type_instance = type_class()
        setattr(type_instance, type_name, instance[0])
        type_instance.probability = instance[1]
        type_instance.duration = RecordingTimeStamps()
        type_instance.duration.start = sqlite_time_to_ros_time(instance[2])
        type_instance.duration.finish = sqlite_time_to_ros_time(instance[3])
        ros_type_list.append(type_instance)

    return ros_type_list


def _get_ssl_results(start_time, finish_time, path):
    start_time_string = ros_time_to_sqlite_time(start_time)
    finish_time_string = ros_time_to_sqlite_time(finish_time)

    # query all elements of type in question
    sql_query = """
    SELECT ssl.ssl_key, time_from, time_to, sourceId, angleVertical, angleHorizontal FROM 
    ssl 
    LEFT JOIN ssl_combo ON ssl.ssl_key = ssl_combo.ssl_key
    LEFT JOIN ssl_dir ON ssl_dir.dir_key = ssl_combo.dir_key
    WHERE 
      time_from BETWEEN "{time_from}" AND "{time_to}"
      OR
      time_to BETWEEN "{time_from}" AND "{time_to}";
    """.format(time_from=start_time_string, time_to=finish_time_string)

    connection = sqlite3.connect(path)
    cursor = connection.cursor()

    cursor.execute(sql_query)
    result_raw = cursor.fetchall()

    connection.commit()
    connection.close()

    # edge case: no entry yet
    if len(result_raw) == 1 and not [x for x in result_raw[0] if x is not None]:
        return []

    # recreate ros type instances
    ssl_results = {}
    for entry in result_raw:
        # add sslInfo if key not yet in dict
        ssl_key = entry[0]
        if ssl_key not in ssl_results:
            ssl_results[ssl_key] = SSLInfo()
            time_stamp = RecordingTimeStamps()
            time_stamp.start = sqlite_time_to_ros_time(entry[1])
            time_stamp.finish = sqlite_time_to_ros_time(entry[2])
            ssl_results[ssl_key].duration = time_stamp
            ssl_results[ssl_key].directions = []

        # add sslDir info to sslInfo
        sslDir = SSLDir()
        sslDir.sourceId = entry[3]
        sslDir.angleVertical = entry[4]
        sslDir.angleHorizontal = entry[5]
        ssl_results[ssl_key].directions.append(sslDir)

    return [ssl_results[x] for x in ssl_results]


def _get_speech_rec_results(start_time, finish_time, path):
    start_time_string = ros_time_to_sqlite_time(start_time)
    finish_time_string = ros_time_to_sqlite_time(finish_time)

    # query all elements of type in question
    sql_query = """
        SELECT speech.speech_key, time_from, time_to, recognizedSpeech, probability FROM 
        speech 
        LEFT JOIN speech_combo ON speech.speech_key = speech_combo.speech_key
        LEFT JOIN speech_hypo ON speech_hypo.hypo_key = speech_combo.hypo_key
        WHERE 
          time_from BETWEEN "{time_from}" AND "{time_to}"
          OR
          time_to BETWEEN "{time_from}" AND "{time_to}";
        """.format(time_from=start_time_string, time_to=finish_time_string)

    connection = sqlite3.connect(path)
    cursor = connection.cursor()

    cursor.execute(sql_query)
    result_raw = cursor.fetchall()

    connection.commit()
    connection.close()

    # edge case: no entry yet
    if len(result_raw) == 1 and not [x for x in result_raw[0] if x is not None]:
        return []

    # recreate ros type instances
    speech_results = {}
    for entry in result_raw:
        # add SpeechInfo if key not yet in dict
        speech_key = entry[0]
        if speech_key not in speech_results:
            speech_results[speech_key] = SpeechInfo()
            time_stamp = RecordingTimeStamps()
            time_stamp.start = sqlite_time_to_ros_time(entry[1])
            time_stamp.finish = sqlite_time_to_ros_time(entry[2])
            speech_results[speech_key].duration = time_stamp
            speech_results[speech_key].hypotheses = []

        # add speech hypothesis info to SpeechInfo
        speech_hyp = SpeechHypothesis()
        speech_hyp.recognizedSpeech = entry[3]
        speech_hyp.probability = entry[4]
        speech_results[speech_key].hypotheses.append(speech_hyp)

    return [speech_results[x] for x in speech_results]


def _get_vad_results(start_time, finish_time, path):
    start_time_string = ros_time_to_sqlite_time(start_time)
    finish_time_string = ros_time_to_sqlite_time(finish_time)

    sql_query = """
    SELECT probability, time_from, time_to FROM vad
    WHERE 
      time_from BETWEEN "{time_from}" AND "{time_to}"
      OR
      time_to BETWEEN "{time_from}" AND "{time_to}";
    """.format(time_from=start_time_string, time_to=finish_time_string)
    connection = sqlite3.connect(path)
    cursor = connection.cursor()

    cursor.execute(sql_query)
    result_raw = cursor.fetchall()

    connection.commit()
    connection.close()

    # edge case: no entry yet
    if len(result_raw) == 1 and not [x for x in result_raw[0] if x is not None]:
        return []

    # iterate over all retrieved instances
    ros_type_list = []
    for instance in result_raw:
        # create new type and fill it with raw result
        type_instance = VADInfo()
        type_instance.probability = instance[0]
        type_instance.duration = RecordingTimeStamps()
        type_instance.duration.start = sqlite_time_to_ros_time(instance[1])
        type_instance.duration.finish = sqlite_time_to_ros_time(instance[2])
        ros_type_list.append(type_instance)

    return ros_type_list
