from Fusion import Fusion
import rospy
from esiaf_ros.msg import EsiafRosMsg



class MetaFusion:
    def __init__(self, anchor_type, db_path, designations, topic):
        self.anchor_type = anchor_type
        self.fusions = []
        self.db_path = db_path
        self.designations = designations
        self.publisher = rospy.Publisher(topic, EsiafRosMsg, queue_size=10)

    def add_info(self, designation, information):
        if designation == self.anchor_type:
            self._create_anchor(information)
            return

        finished_fusions = []
        for fusion in self.fusions:
            fusion.new_info(designation, information)
            if not fusion.check_finished():
                finished_fusions.append(fusion)
        for fusion in finished_fusions:
            msg = fusion.create_esiaf_ros_msg()
            self.publisher.publish(msg)
            self.fusions.remove(fusion)



    def _create_anchor(self, information):
        self.fusions.append(Fusion(self.designations, information, self.anchor_type, self.db_path))


