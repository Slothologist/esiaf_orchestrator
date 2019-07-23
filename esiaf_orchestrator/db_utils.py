import os
import sqlite3
from datetime import datetime as dt
import time

from esiaf_ros.msg import AudioTopicFormatConstants as ATFC, VADInfo, SpeechInfo, SSLInfo, GenderInfo, EmotionInfo, \
    VoiceIdInfo
import rospy


DESIGNATION_DICT = {
    ATFC.VAD: ('VAD', VADInfo),
    ATFC.SpeechRec: ('SpeechRec', SpeechInfo),
    ATFC.SSL: ('SSL', SSLInfo),
    ATFC.Gender: ('gender', GenderInfo),
    ATFC.Emotion: ('emotion', EmotionInfo),
    ATFC.VoiceId: ('voiceId', VoiceIdInfo)
}


def integer_to_ros_duration(integer):
    return rospy.Duration(integer // int(1e9), integer % int(1e9))


def ros_time_to_sqlite_time(ros_time):
    return dt.fromtimestamp(ros_time.to_sec())


def sqlite_time_to_ros_time(sql_time):
    date_time = dt.strptime(sql_time, "%Y-%m-%d %H:%M:%S.%f")
    time_time = time.mktime(date_time.timetuple()) + date_time.microsecond/1e6
    ros_time = rospy.Time(time_time)
    return ros_time


def create_db(path):
    connection = sqlite3.connect(path)
    cursor = connection.cursor()

    _table_creation(cursor)

    connection.commit()
    connection.close()


def db_startup(path):
    # remove old file if it exists
    if os.path.exists(path) and os.path.isfile(path):
        os.remove(path)
    # create new db in place
    create_db(path)


def _table_creation(cursor):

    create_speech = """
    CREATE TABLE speech ( 
    speech_key INTEGER PRIMARY KEY,
    time_from TEXT,
    time_to TEXT,
    latency INTEGER);"""

    create_speech_hypo = """
    CREATE TABLE speech_hypo ( 
    hypo_key INTEGER PRIMARY KEY, 
    recognizedSpeech VARCHAR(500), 
    probability REAL);"""

    create_speech_combo = """
    CREATE TABLE speech_combo ( 
    combo_key INTEGER PRIMARY KEY, 
    hypo_key INTEGER, 
    speech_key INTEGER);"""


    create_emotion = """
    CREATE TABLE emotion ( 
    emotion_key INTEGER PRIMARY KEY,
    emotion VARCHAR(20),
    probability REAL,
    time_from TEXT,
    time_to TEXT,
    latency INTEGER);"""


    create_gender = """
    CREATE TABLE gender ( 
    gender_key INTEGER PRIMARY KEY,
    gender VARCHAR(20),
    probability REAL,
    time_from TEXT,
    time_to TEXT,
    latency INTEGER);"""


    create_voiceID = """
    CREATE TABLE voiceId ( 
    voiceId_key INTEGER PRIMARY KEY,
    voiceId VARCHAR(20),
    probability REAL,
    time_from TEXT,
    time_to TEXT,
    latency INTEGER);"""


    create_ssl = """
    CREATE TABLE ssl (
    ssl_key INTEGER PRIMARY KEY,
    time_from TEXT,
    time_to TEXT,
    latency INTEGER);"""

    create_ssl_dir = """
    CREATE TABLE ssl_dir ( 
    dir_key INTEGER PRIMARY KEY, 
    sourceId VARCHAR(20),  
    angleVertical REAL,  
    angleHorizontal REAL, 
    probability REAL);"""

    create_ssl_combo = """
    CREATE TABLE ssl_combo ( 
    combo_key INTEGER PRIMARY KEY, 
    dir_key INTEGER, 
    ssl_key INTEGER);"""


    cursor.execute(create_speech)
    cursor.execute(create_speech_hypo)
    cursor.execute(create_speech_combo)
    cursor.execute(create_emotion)
    cursor.execute(create_gender)
    cursor.execute(create_voiceID)
    cursor.execute(create_ssl)
    cursor.execute(create_ssl_dir)
    cursor.execute(create_ssl_combo)
