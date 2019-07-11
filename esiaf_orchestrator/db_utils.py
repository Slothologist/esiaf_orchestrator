import os
import sqlite3


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
    time_to TEXT);"""

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
    time_to TEXT);"""


    create_gender = """
    CREATE TABLE gender ( 
    gender_key INTEGER PRIMARY KEY,
    gender VARCHAR(20),
    probability REAL,
    time_from TEXT,
    time_to TEXT);"""


    create_voiceID = """
    CREATE TABLE voideID ( 
    voiceID_key INTEGER PRIMARY KEY,
    voideID VARCHAR(20),
    probability REAL,
    time_from TEXT,
    time_to TEXT);"""


    create_ssl = """
    CREATE TABLE ssl (
    ssl_key INTEGER PRIMARY KEY,
    time_from TEXT,
    time_to TEXT);"""

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
