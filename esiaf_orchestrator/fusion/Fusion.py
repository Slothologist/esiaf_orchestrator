
from ..db_utils import DESIGNATION_DICT
from esiaf_ros.msg import *
from ..db_retrieval import get_results


class Fusion:
    def __init__(self, designations, anchor, anchortype, db_path):
        self.designations = designations
        self.information = {anchortype: [anchor]}
        self.duration = anchor.duration
        self.anchortype = anchortype
        self.db_path = db_path
        self._gather_already_existing_info()

    def new_info(self, designation, information):
        if not self._determine_relevance(information):
            return
        self.information[designation].append(information)

    def _determine_relevance(self, information):
        if information.duration.start < self.duration.start < information.duration.finish:
            return True
        if information.duration.start < self.duration.finish < information.duration.finish:
            return True
        return False

    def _gather_already_existing_info(self):
        basic_designations = [x for x in self.designations if x not in ['VAD', self.anchortype]]
        for designation in basic_designations:
            type_name = DESIGNATION_DICT[designation][0]
            self.information[type_name], _ = get_results(type_name, self.duration.start, self.duration.finish, self.db_path)

    def create_esiaf_ros_msg(self):
        msg = EsiafRosMsg()
        attribute_dict = {
            'VAD': 'vadInfo',
            'SpeechRec': 'speechInfo',
            'SSL': 'sslInfo',
            'gender': 'genderInfo',
            'emotion': 'emotionInfo',
            'voiceId': 'voiceIdInfo'
        }
        for information_type in self.information:
            setattr(msg, attribute_dict[information_type], self.information[information_type])
        return msg

    def check_finished(self):
        return True

