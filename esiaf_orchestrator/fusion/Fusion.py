import rospy
from ..db_utils import DESIGNATION_DICT
from esiaf_ros.msg import *
from ..db_retrieval import get_results


class Fusion:
    def __init__(self, designations, anchor, anchortype, db_path):
        self.designations = designations
        self.anchortype = anchortype
        self.information = {self.anchortype: [anchor]}
        self.duration = anchor.duration
        self.db_path = db_path
        self.latencies = {}
        self._gather_already_existing_info()

    def new_info(self, designation, information):
        if not self._determine_relevance(information):
            rospy.logdebug('non relevant information')
            return
        self.information[designation].append(information)

    def _determine_relevance(self, information):
        if information.duration.start <= self.duration.start < information.duration.finish:
            return True
        if information.duration.start < self.duration.finish <= information.duration.finish:
            return True
        return False

    def _gather_already_existing_info(self):
        basic_designations = [DESIGNATION_DICT[x][0] for x in self.designations if DESIGNATION_DICT[x][0] not in [self.anchortype]]
        for designation in basic_designations:
            self.information[designation], self.latencies[designation] = get_results(designation, self.duration.start, self.duration.finish, self.db_path)

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
        rospy.logdebug('check finished info: ' + str(self.information))
        anchor_end = self.information[self.anchortype][0].duration.finish
        time_since_anchor = rospy.get_rostime() - anchor_end

        # time based check
        for each in self.latencies:
            diff = self.latencies[each]
            diff_threshold = rospy.Duration.from_sec(float(str(1.5*diff))/1e10)  # workaround for genpy/ rospy conversion
            if time_since_anchor > diff_threshold:
                rospy.logdebug('time threshold reached, threshold is: ' + str(diff_threshold))
                return True

        # check based on latest information
        for each in self.latencies:
            infos = self.information[each]
            lastest_info_after_anchor_end = False
            for info in infos:
                if info.duration.finish >= anchor_end:
                    lastest_info_after_anchor_end = True
            if not lastest_info_after_anchor_end:
                rospy.logdebug('missing information: ' + str(each))
                return False

        return True
