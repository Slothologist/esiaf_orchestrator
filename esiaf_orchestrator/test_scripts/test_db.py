from db_utils import create_db, DESIGNATION_DICT, sqlite_time_to_ros_time, ros_time_to_sqlite_time
from SubMsgSubscriber import SubMsgSubscriber
import datetime
from esiaf_ros.msg import SSLInfo, SSLDir, SpeechInfo, SpeechHypothesis, RecordingTimeStamps, GenderInfo, EmotionInfo, \
    VoiceIdInfo

# setup stuff

db_path = 'database.db'

time_one_org = datetime.datetime(2018, 1, 1, 12, 0, 0, 1)
time_two_org = datetime.datetime(2018, 1, 1, 12, 0, 1, 100)

time_one = sqlite_time_to_ros_time(str(time_one_org))
time_two = sqlite_time_to_ros_time(str(time_two_org))

time_stamps = RecordingTimeStamps()
time_stamps.start = time_one
time_stamps.finish = time_two

create_db(db_path)

gender_msg = GenderInfo()
gender_msg.gender = 'male'
gender_msg.probability = 0.9
gender_msg.duration = time_stamps

emotion_msg = EmotionInfo()
emotion_msg.emotion = 'neutral'
emotion_msg.probability = 0.8
emotion_msg.duration = time_stamps

voice_msg = VoiceIdInfo()
voice_msg.voiceId = '15'
voice_msg.probability = 0.7
voice_msg.duration = time_stamps

subscriber = SubMsgSubscriber('bla', 'blubb', db_path)


################### test stuff

# gender test write
msg = gender_msg
dict = {'Gender': msg.gender,
        'probability': msg.probability,
        'from': ros_time_to_sqlite_time(msg.duration.start),
        'to': ros_time_to_sqlite_time(msg.duration.finish)}
subscriber._simple_write_to_db(dict, 'Gender')

# emotion test write
msg = emotion_msg
dict = {'Emotion': msg.emotion,
        'probability': msg.probability,
        'from': ros_time_to_sqlite_time(msg.duration.start),
        'to': ros_time_to_sqlite_time(msg.duration.finish)}
subscriber._simple_write_to_db(dict, 'Emotion')

# voiceId test write
msg = voice_msg
dict = {'VoiceID': msg.voiceId,
        'probability': msg.probability,
        'from': ros_time_to_sqlite_time(msg.duration.start),
        'to': ros_time_to_sqlite_time(msg.duration.finish)}
subscriber._simple_write_to_db(dict, 'VoiceID')
