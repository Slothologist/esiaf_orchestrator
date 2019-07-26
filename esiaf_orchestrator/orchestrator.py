# ros imports
import rosnode
import rospy

# esiaf imports
from esiaf_ros.msg import *
from esiaf_ros.srv import *

# threading imports
import threading
import time
import signal
import sys

# util imports
from .node import Node
from esiaf_orchestrator.fusion.MetaFusion import MetaFusion
from esiaf_orchestrator.db_utils import DESIGNATION_DICT
import utils


class Orchestrator:
    """
    Class representing a esiaf_ros orchestrator. It will handle orchestration between the esiaf_ros nodes.
    This includes registering of nodes, determining their audio in- and output formats and the points in the audio flow
    where resampling is necessary.
    """


    def __init__(self,
                 db_path,
                 meta_fusion_config,
                 remove_dead_rate=0.2,
                 resampling_strategy='minimise_network_traffic',
                 fusion_check_rate=0.2
                 ):
        """
        Will initialise an orchestrator according to the config file found under the given path.
        :param path_to_config: may be relative or absolute
        """
        self.active_nodes = []
        self.active_nodes_lock = threading.Lock()
        self.resampling_strategy = resampling_strategy
        self.db_path = db_path
        self.stopping_signal = False
        self.meta_fusions = self.generate_metafusions_from_config(meta_fusion_config, db_path, fusion_check_rate)
        self.registerService = rospy.Service('/esiaf_ros/orchestrator/register', RegisterNode, self.register_node)

        # define loop function for removing dead nodes and start a thread calling it every X seconds
        def dead_loop(orc_instance, stop):
            while not stop():
                Orchestrator.remove_dead_nodes(orc_instance)
                time.sleep(1 / remove_dead_rate)
            sys.exit(0)

        t = threading.Thread(target=dead_loop, args=(self, lambda: self.stopping_signal))
        t.start()

        # create a signal handler so that sigint does not take forever to escalate and eventually kill the orchestrator
        def signal_handler(sig, frame):
            rospy.loginfo('Got SIGINT, shutting down!')
            self.stopping_signal = True
            t.join()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

    def remove_dead_nodes(self):
        """
        This function will remove nodes from the orchestrators active_nodes list which have shut down.
        Should be invoked in a regular interval.
        :return: None
        """
        momentary_nodenames = rosnode.get_node_names()

        with self.active_nodes_lock:
            def check_node_ping(name):
                try:
                    return rosnode.rosnode_ping(name, 1)
                except:
                    return False

            still_active_nodes = [x for x in self.active_nodes if
                                  x.name in momentary_nodenames]
            dead_nodes = [x for x in self.active_nodes if
                          x.name not in momentary_nodenames or not check_node_ping(x.name)]
            crashed_nodes = [x for x in self.active_nodes if not check_node_ping(x.name)]

            if dead_nodes:
                rospy.logdebug('One or more esiaf nodes shut down!')
                for dead in dead_nodes:
                    dead.bury()
                for crashed in crashed_nodes:
                    if crashed in still_active_nodes:
                        still_active_nodes.remove(crashed)
                self.active_nodes = still_active_nodes
                self.calculate_audio_tree()

    def register_node(self, nodeinfo):
        """
        The callback function for the Orchestrators register subscription. Will register a node with this Orchestrator.
        :param nodeinfo: the ros message containing the registering node's information
        :return: 
        """
        new_node = Node(nodeinfo, self.db_path, self.meta_fusions)
        rospy.loginfo('Registering new node: \n' + str(nodeinfo))

        with self.active_nodes_lock:
            self.active_nodes.append(new_node)
            self.calculate_audio_tree()

        return RegisterNodeResponse()

    def best_format(self, input_formats, output_formats):
        resampling_strategy_dict = {'minimise_network_traffic': utils.best_format_traffic_min,
                                    'minimise_cpu_usage': utils.best_format_cpu_min}
        return resampling_strategy_dict[self.resampling_strategy](input_formats, output_formats)

    def calculate_audio_tree(self):
        """
        Creates a new, optimized audio flow tree based on node preferences.
        The node's determined audio formats will be stored directly in them, no special tree datastructure is constructed.
        Assumes to already have the active_nodes_lock.
        :return: None
        """

        def __add_entry(topic_name, node_input=None, node_output=None):
            if topic_name not in topic_deps:
                topic_deps[topic_name] = [[],[]]
            if node_input:
                topic_deps[topic_name][0].append(node_input)
            if node_output:
                topic_deps[topic_name][1].append(node_output)

        def __get_format_per_topicname(allowedTopics, name):
            for topic in allowedTopics:
                #rospy.loginfo('allowedtopics has: ' + topic.topic)
                if topic.topic == name:
                    return topic.allowedFormat

        topic_deps = {}

        # populate topic dependency dict
        for node in self.active_nodes:
            for intopic in node.allowedTopicsIn:
                __add_entry(intopic.topic, node_input=node)
            for outtopic in node.allowedTopicsOut:
                __add_entry(outtopic.topic, node_output=node)

        rospy.loginfo('Topic deps is: ')
        rospy.loginfo(str(topic_deps))

        best_format_per_topic = {}

        # aquire best format for use on each topic
        for topic in topic_deps:
            input_nodes, output_nodes = topic_deps[topic]
            input_formats = [__get_format_per_topicname(node.allowedTopicsIn, topic) for node in input_nodes]
            output_formats = [__get_format_per_topicname(node.allowedTopicsOut, topic) for node in output_nodes]

            best_format_per_topic[topic] = self.best_format(input_formats, output_formats)

        rospy.loginfo('best format per topic is: ')
        rospy.loginfo(str(best_format_per_topic))

        # set the best audio format for each node and topic
        for node in self.active_nodes:
            old_actual_IN = node.actualTopicsIn
            node.actualTopicsIn = []
            old_actual_OUT = node.actualTopicsOut
            node.actualTopicsOut = []
            for IN_topicinfo in node.allowedTopicsIn:
                node.actualTopicsIn.append((IN_topicinfo.topic, best_format_per_topic[IN_topicinfo.topic]))
            for OUT_topicinfo in node.allowedTopicsOut:
                node.actualTopicsOut.append((OUT_topicinfo.topic, best_format_per_topic[OUT_topicinfo.topic]))

            if old_actual_IN == node.actualTopicsIn and old_actual_OUT == node.actualTopicsOut:
                pass
            else:
                node.update_config()

    def generate_metafusions_from_config(self, config, db_path, fusion_check_rate):
        anchor_type_dict = {DESIGNATION_DICT[x][0]: DESIGNATION_DICT[x][1] for x in DESIGNATION_DICT}
        designation_dict = {DESIGNATION_DICT[x][0]: x for x in DESIGNATION_DICT}
        meta_fusions = []
        for each in config:
            m_fusion_ = config[each]
            meta_fusions.append(MetaFusion(
                anchor_type_dict[m_fusion_['anchor_type']],
                db_path,
                [designation_dict[designation_str] for designation_str in m_fusion_['designations']],
                m_fusion_['output_topic'],
                fusion_check_rate,
                lambda: self.stopping_signal
            ))
        return meta_fusions
