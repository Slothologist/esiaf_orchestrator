# ros imports
import rospy

# audio info imports
from .AudioInfo import AudioTopicInfo
from esiaf_ros.msg import ChangedConfig, AudioTopicInfo as esiaf_ATI
from .SubMsgSubscriber import SubMsgSubscriber
from db_utils import DESIGNATION_DICT



class Node:
    """
    Class representing a ROS Node inside the esiaf_ros pipeline.
    """

    name = None
    designation = None
    subMsgSubscriber = None
    configPublisher = None

    allowedTopicsIn = None
    allowedTopicsOut = None

    actualTopicsIn = None
    actualTopicsOut = None

    def __init__(self, nodeinfo, db_path, meta_fusions):
        self.name = nodeinfo.name
        self.designation = nodeinfo.designation
        self.configPublisher = rospy.Publisher('/esiaf_ros' + self.name + '/changedConfig', ChangedConfig, queue_size=10, latch=True)

        self.allowedTopicsIn = []
        self.allowedTopicsOut = []

        self.actualTopicsIn = []
        self.actualTopicsOut = []

        for allowedTopicIn in nodeinfo.inputTopics:
            self.allowedTopicsIn.append(AudioTopicInfo(allowedTopicIn))

        for allowedTopicOut in nodeinfo.outputTopics:
            self.allowedTopicsOut.append(AudioTopicInfo(allowedTopicOut))

        if self.designation in DESIGNATION_DICT:
            self.subMsgSubscriber = SubMsgSubscriber(self.name, self.designation, db_path, meta_fusions)


    def update_config(self):
        """
        Sends out this node-representations momentary, actual configuration to the real node.
        :return: None
        """
        config = ChangedConfig()

        config.inputTopics = []
        for topic in self.actualTopicsIn:
            determinedConfig = esiaf_ATI()
            determinedConfig.topic = topic[0]
            determinedConfig.allowedFormat = topic[1].to_ros()
            config.inputTopics.append(determinedConfig)

        config.outputTopics = []
        for topic in self.actualTopicsOut:
            determinedConfig = esiaf_ATI()
            determinedConfig.topic = topic[0]
            determinedConfig.allowedFormat = topic[1].to_ros()
            config.outputTopics.append(determinedConfig)

        self.configPublisher.publish(config)
        rospy.logdebug('Providing new config for "' + self.name + '": ' + str(config))

    def bury(self):
        """
        Basically a destructor for dead nodes. Will kill their publishers and subscribers.
        :return:
        """
        self.configPublisher.unregister()
        if self.subMsgSubscriber:
            self.subMsgSubscriber.bury()
        rospy.loginfo('Node %s has vanished. Removing it from pipeline!' % self.name)
        del self

    def __str__(self):
        string = 'Node [\n\t'
        string += 'Nodename: {}\n\t'.format(self.name)
        string += 'Designation: {}\n\t'.format(self.designation)
        string += 'Allowed Input topics: {}\n\t'.format(self.allowedTopicsIn)
        string += 'Allowed Output topics: {}\n\t'.format(self.allowedTopicsOut)
        string += 'Actual Input topics: {}\n\t'.format(self.actualTopicsIn)
        string += 'Actual Output topics: {}\n'.format(self.actualTopicsOut)
        string += ']'
        return string
