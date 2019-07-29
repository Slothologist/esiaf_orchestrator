# ros imports
import rospy

# msg imports
from esiaf_ros.msg import AudioTopicFormatConstants as ATFC
from db_utils import DESIGNATION_DICT

# db imports
import sqlite3
from db_utils import ros_time_to_sqlite_time


class SubMsgSubscriber:

    def __init__(self,
                 name,
                 designation,
                 db_path,
                 meta_fusions):
        self.designation = designation
        self.db_path = db_path
        self.meta_fusions = meta_fusions
        if designation in DESIGNATION_DICT:
            self.subscriber = rospy.Subscriber(self._fix_name(name) + '/' + DESIGNATION_DICT[designation][0],
                                               DESIGNATION_DICT[designation][1],
                                               self.callback)

    def callback(self, msg):
        for meta_fusion in self.meta_fusions:
            meta_fusion.add_info(self.designation, msg)

        if self.designation == ATFC.VAD:
            self._write_vad_to_db(msg)
        elif self.designation == ATFC.SpeechRec:
            self._write_speech_to_db(msg)
        elif self.designation == ATFC.SSL:
            self._write_ssl_to_db(msg)
        elif self.designation == ATFC.Gender:
            dict = {'gender': msg.gender,
                    'probability': msg.probability,
                    'from': ros_time_to_sqlite_time(msg.duration.start),
                    'to': ros_time_to_sqlite_time(msg.duration.finish),
                    'latency': int(str(rospy.get_rostime() - msg.duration.finish))}
            self._simple_write_to_db(dict, 'gender')
        elif self.designation == ATFC.Emotion:
            dict = {'emotion': msg.emotion,
                    'probability': msg.probability,
                    'from': ros_time_to_sqlite_time(msg.duration.start),
                    'to': ros_time_to_sqlite_time(msg.duration.finish),
                    'latency': int(str(rospy.get_rostime() - msg.duration.finish))}
            self._simple_write_to_db(dict, 'emotion')
        elif self.designation == ATFC.VoiceId:
            dict = {'voiceId': msg.voiceId,
                    'probability': msg.probability,
                    'from': ros_time_to_sqlite_time(msg.duration.start),
                    'to': ros_time_to_sqlite_time(msg.duration.finish),
                    'latency': int(str(rospy.get_rostime() - msg.duration.finish))}
            self._simple_write_to_db(dict, 'voiceId')

    def _simple_write_to_db(self, object_dict, type):
        sql_command = """
              INSERT INTO {type} ({type}_key, {type}, probability, time_from, time_to, latency)
              VALUES 
                    (NULL,
                    "{value}", 
                    "{prob}", 
                    "{time_from}", 
                    "{time_to}",
                    "{latency}");
                    """.format(type=type,
                               value=object_dict[type],
                               prob=object_dict['probability'],
                               time_from=object_dict['from'],
                               time_to=object_dict['to'],
                               latency=object_dict['latency']
                               )
        rospy.logdebug('latency while writing to database: ' + str(object_dict['latency']) + ', type: ' + type)

        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()

        cursor.execute(sql_command)

        connection.commit()
        connection.close()

    def _write_speech_to_db(self, msg):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()

        # prepare main entry commands
        speech_hypo_command = """
        INSERT INTO speech_hypo (hypo_key, recognizedSpeech, probability)
        VALUES 
        (NULL, "{recognizedSpeech}", "{probability}");
        """
        speech_command = """
        INSERT INTO speech (speech_key, time_from, time_to, latency)
        VALUES (NULL, "{time_from}", "{time_to}", "{latency}");
        """

        speech_combo_command = """
        INSERT INTO speech_combo (combo_key, hypo_key, speech_key)
        VALUES (NULL, "{hypo}", "{speech}");
        """

        # write all hypothesis
        hypo_ids = []
        for hypo in msg.hypotheses:
            cursor.execute(speech_hypo_command.format(recognizedSpeech=hypo.recognizedSpeech,
                                                      probability=hypo.probability))
            hypo_ids.append(cursor.lastrowid)

        # write the main speech entry
        cursor.execute(speech_command.format(time_from=ros_time_to_sqlite_time(msg.duration.start),
                                             time_to=ros_time_to_sqlite_time(msg.duration.finish),
                                             latency=int(str(rospy.get_rostime() - msg.duration.finish))))
        speech_key = cursor.lastrowid

        # write the compound entries
        for hypo in hypo_ids:
            cursor.execute(speech_combo_command.format(hypo=hypo, speech=speech_key))

        connection.commit()
        connection.close()

    def _write_ssl_to_db(self, msg):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()

        # prepare main entry commands
        ssl_dir_command = """
        INSERT INTO ssl_dir (dir_key, sourceId, angleVertical, angleHorizontal)
        VALUES 
        (NULL, "{sourceId}", "{angleVertical}", "{angleHorizontal}");
        """

        ssl_command = """
        INSERT INTO ssl (ssl_key, time_from, time_to, latency)
        VALUES (NULL, "{time_from}", "{time_to}", "{latency}");
        """

        ssl_combo_command = """
        INSERT INTO ssl_combo (combo_key, ssl_key, dir_key)
        VALUES (NULL, "{ssl}", "{dir}");
        """

        # write all directions
        dir_ids = []
        for dir in msg.directions:
            cursor.execute(ssl_dir_command.format(sourceId=dir.sourceId,
                                                  angleVertical=dir.angleVertical,
                                                  angleHorizontal=dir.angleHorizontal))
            dir_ids.append(cursor.lastrowid)

        # write the main ssl entry
        cursor.execute(ssl_command.format(time_from=ros_time_to_sqlite_time(msg.duration.start),
                                          time_to=ros_time_to_sqlite_time(msg.duration.finish),
                                          latency=int(str(rospy.get_rostime() - msg.duration.finish))))
        ssl_key = cursor.lastrowid

        # write the compound entries
        for dir in dir_ids:
            cursor.execute(ssl_combo_command.format(ssl=ssl_key, dir=dir))

        connection.commit()
        connection.close()

    def _write_vad_to_db(self, msg):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()

        sql_command = """
                      INSERT INTO vad (vad_key, probability, time_from, time_to, latency)
                      VALUES 
                            (NULL, 
                            "{prob}", 
                            "{time_from}", 
                            "{time_to}",
                            "{latency}");
                            """.format(
                                       prob=msg.probability,
                                       time_from=ros_time_to_sqlite_time(msg.duration.start),
                                       time_to=ros_time_to_sqlite_time(msg.duration.finish),
                                       latency=int(str(rospy.get_rostime() - msg.duration.finish))
                                       )

        cursor.execute(sql_command)

        connection.commit()
        connection.close()

    def bury(self):
        if self.subscriber:
            self.subscriber.unregister()

    @staticmethod
    def _fix_name(name):
        """
        This method is needed for python nodes, which due to usage of both rospy and roscpp in the esiaf ros library
        :param name:
        :return:
        """
        fixed_name = name
        if '_' in name:
            if not [x for x in name.split('_')[-1] if x not in [str(y) for y in range(10)]]: # check if all characters after the split are numbers
                fixed_name = '_'.join(name.split('_')[:-1])
        return fixed_name
