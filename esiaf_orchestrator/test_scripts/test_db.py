from esiaf_orchestrator.db_utils import db_startup, sqlite_time_to_ros_time, ros_time_to_sqlite_time, integer_to_ros_duration
from esiaf_orchestrator.SubMsgSubscriber import SubMsgSubscriber
import datetime
from esiaf_ros.msg import SSLInfo, SSLDir, SpeechInfo, SpeechHypothesis, RecordingTimeStamps, GenderInfo, EmotionInfo, \
    VoiceIdInfo, VADInfo
from esiaf_orchestrator.db_retrieval import _get_basic_results, _get_speech_rec_results, _get_ssl_results, _get_vad_results
import rospy

# setup stuff

rospy.init_node('testnode')

dur = rospy.Duration(5)
dur2 = integer_to_ros_duration(int(str(dur)))
assert dur == dur2

db_path = 'database.db'

time_one_org = datetime.datetime(2018, 1, 1, 12, 0, 0, 1)
time_two_org = datetime.datetime(2018, 1, 1, 12, 0, 1, 100)

time_one = sqlite_time_to_ros_time(str(time_one_org))
time_two = sqlite_time_to_ros_time(str(time_two_org))

time_stamps = RecordingTimeStamps()
time_stamps.start = time_one
time_stamps.finish = time_two

db_startup(db_path, True)

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

vad_msg = VADInfo()
vad_msg.probability = 0.3
vad_msg.duration = time_stamps



subscriber = SubMsgSubscriber('bla', 'blubb', db_path, [])


################### test stuff

# test read before write

retrieved, latency = _get_basic_results('gender', time_one, time_two, db_path)
assert (retrieved, latency) == ([], 0)
retrieved, latency = _get_basic_results('emotion', time_one, time_two, db_path)
assert (retrieved, latency) == ([], 0)
retrieved, latency = _get_basic_results('voiceId', time_one, time_two, db_path)
assert (retrieved, latency) == ([], 0)
retrieved, latency = _get_speech_rec_results(time_one, time_two, db_path)
assert (retrieved, latency) == ([], 0)
retrieved, latency = _get_ssl_results(time_one, time_two, db_path)
assert (retrieved, latency) == ([], 0)
retrieved, latency = _get_vad_results(time_one, time_two, db_path)
assert (retrieved, latency) == ([], 0)

#### basic stuff test

# gender test write
msg = gender_msg
dict = {'gender': msg.gender,
        'probability': msg.probability,
        'from': ros_time_to_sqlite_time(msg.duration.start),
        'to': ros_time_to_sqlite_time(msg.duration.finish),
        'latency': int(str(rospy.get_rostime() - msg.duration.finish))}
subscriber._simple_write_to_db(dict, 'gender')

# gender test read
retrieved, latency = _get_basic_results('gender', time_one, time_two, db_path)
assert retrieved[0] == msg

# emotion test write
msg = emotion_msg
dict = {'emotion': msg.emotion,
        'probability': msg.probability,
        'from': ros_time_to_sqlite_time(msg.duration.start),
        'to': ros_time_to_sqlite_time(msg.duration.finish),
        'latency': int(str(rospy.get_rostime() - msg.duration.finish))}
subscriber._simple_write_to_db(dict, 'emotion')

# emotion test read
retrieved, latency = _get_basic_results('emotion', time_one, time_two, db_path)
assert retrieved[0] == msg

# voiceId test write
msg = voice_msg
dict = {'voiceId': msg.voiceId,
        'probability': msg.probability,
        'from': ros_time_to_sqlite_time(msg.duration.start),
        'to': ros_time_to_sqlite_time(msg.duration.finish),
        'latency': int(str(rospy.get_rostime() - msg.duration.finish))}
subscriber._simple_write_to_db(dict, 'voiceId')

# voiceId test read
retrieved, latency = _get_basic_results('voiceId', time_one, time_two, db_path)
assert retrieved[0] == msg


#### speech tests

# write
subscriber._write_speech_to_db(speech_msg)

# read
retrieved, latency = _get_speech_rec_results(time_one, time_two, db_path)
assert retrieved[0] == speech_msg

#### ssl tests

# write
subscriber._write_ssl_to_db(ssl_msg)

# read
retrieved, latency = _get_ssl_results(time_one, time_two, db_path)
assert retrieved[0] == ssl_msg

#### vad tests

#write
subscriber._write_vad_to_db(vad_msg)

#read
retrieved, latency = _get_vad_results(time_one, time_two, db_path)
assert retrieved[0] == vad_msg

print('Tests passed')
