from Fusion import Fusion
import rospy
from esiaf_ros.msg import EsiafRosMsg
from esiaf_orchestrator.db_utils import DESIGNATION_DICT
import threading
import time


class MetaFusion:
    def __init__(self, anchor_type, db_path, designations, topic, check_rate, stopping_signal):
        self.anchor_type = anchor_type
        self.fusions = []
        self.db_path = db_path
        self.designations = designations
        self.publisher = rospy.Publisher(topic, EsiafRosMsg, queue_size=10)
        self.lock = threading.Lock()
        self.check_rate = check_rate
        self.stopping_signal = stopping_signal
        self.timer = threading.Thread(target=MetaFusion._check_timer, args=(self, self.stopping_signal))
        rospy.logdebug('Metafusion created, anchor_type: ' + self.anchor_type)

    def add_info(self, designation, information):
        rospy.logdebug('Aquiring new information, info-type: ' + str(type(information)))
        with self.lock:
            if DESIGNATION_DICT[designation][0] == self.anchor_type:
                self._create_anchor(information)
                self._check_fusions()
                return

            for fusion in self.fusions:
                fusion.new_info(DESIGNATION_DICT[designation][0], information)

            self._check_fusions()

    def _create_anchor(self, information):
        rospy.logdebug('Creating new fusion!')
        self.fusions.append(Fusion(self.designations, information, self.anchor_type, self.db_path))

    def _check_timer(self, stop):
        while not stop():
            time.sleep(1/self.check_rate)
            with self.lock:
                rospy.loginfo('checking fusions')
                self._check_fusions()

    def _check_fusions(self):
        finished_fusions = []
        for fusion in self.fusions:
            if fusion.check_finished():
                finished_fusions.append(fusion)
        for fusion in finished_fusions:
            msg = fusion.create_esiaf_ros_msg()
            self.publisher.publish(msg)
            self.fusions.remove(fusion)
