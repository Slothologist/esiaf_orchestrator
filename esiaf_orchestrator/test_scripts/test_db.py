from esiaf_orchestrator.db_utils import create_db, sqlite_time_to_ros_time, ros_time_to_sqlite_time
from esiaf_orchestrator.SubMsgSubscriber import SubMsgSubscriber
import datetime
from esiaf_ros.msg import SSLInfo, SSLDir, SpeechInfo, SpeechHypothesis, RecordingTimeStamps, GenderInfo, EmotionInfo, \
    VoiceIdInfo
from esiaf_orchestrator.db_retrieval import _get_basic_results, _get_speech_rec_results, _get_ssl_results

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

speech_msg = SpeechInfo()
speech_msg.duration = time_stamps
speech_msg.hypotheses = []
speech_msg.hypotheses.append(SpeechHypothesis())
speech_msg.hypotheses[0].recognizedSpeech = 'hello robot'
speech_msg.hypotheses[0].probability = 0.6


ssl_msg = SSLInfo()
ssl_msg.duration = time_stamps
ssl_msg.directions = []
ssl_msg.directions.append(SSLDir())
ssl_msg.directions[0].sourceId = 'source 1'
ssl_msg.directions[0].angleVertical = 0.5
ssl_msg.directions[0].angleHorizontal = 0.4


subscriber = SubMsgSubscriber('bla', 'blubb', db_path)


################### test stuff

#### basic stuff test

# gender test write
msg = gender_msg
dict = {'gender': msg.gender,
        'probability': msg.probability,
        'from': ros_time_to_sqlite_time(msg.duration.start),
        'to': ros_time_to_sqlite_time(msg.duration.finish)}
subscriber._simple_write_to_db(dict, 'gender')

# gender test read
retrieved = _get_basic_results('gender', time_one, time_two, db_path)
assert retrieved[0] == msg

# emotion test write
msg = emotion_msg
dict = {'emotion': msg.emotion,
        'probability': msg.probability,
        'from': ros_time_to_sqlite_time(msg.duration.start),
        'to': ros_time_to_sqlite_time(msg.duration.finish)}
subscriber._simple_write_to_db(dict, 'emotion')

# emotion test read
retrieved = _get_basic_results('emotion', time_one, time_two, db_path)
assert retrieved[0] == msg

# voiceId test write
msg = voice_msg
dict = {'voiceId': msg.voiceId,
        'probability': msg.probability,
        'from': ros_time_to_sqlite_time(msg.duration.start),
        'to': ros_time_to_sqlite_time(msg.duration.finish)}
subscriber._simple_write_to_db(dict, 'voiceId')

# voiceId test read
retrieved = _get_basic_results('voiceId', time_one, time_two, db_path)
assert retrieved[0] == msg


#### speech tests

# write
subscriber._write_speech_to_db(speech_msg)

# read
retrieved = _get_speech_rec_results(time_one, time_two, db_path)
assert retrieved[0] == speech_msg

#### ssl tests

# write
subscriber._write_ssl_to_db(ssl_msg)

# read
retrieved = _get_ssl_results(time_one, time_two, db_path)
assert retrieved[0] == ssl_msg

print('Tests passed')
