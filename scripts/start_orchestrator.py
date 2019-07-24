#!/usr/bin/env python

from esiaf_orchestrator.orchestrator import Orchestrator
from esiaf_orchestrator.db_utils import db_startup
from esiaf_ros.msg import *
import rospy

# config
import yaml
import sys

# initialize rosnode
rospy.init_node('orchestrator')


# read config
rospy.loginfo('Loading config...')
argv = sys.argv
if len(argv) < 2:
    rospy.logerr('Need path to configfile as first parameter!')
    exit('1')
path_to_config = argv[1]
data = yaml.safe_load(open(path_to_config))

clean_db_on_startup=data['clean_db_on_startup']
db_startup(data['db_path'], clean_db_on_startup)

# create the orchestrator
rospy.loginfo('Creating Orchestrator...')
orc = Orchestrator(
    db_path=data['db_path'],
    remove_dead_rate=data['remove_dead_rate'],
    resampling_strategy=data['resampling_strategy'],
    fusion_check_rate=data['fusion_check_rate']
)

rospy.loginfo('Orchestrator ready!')
rospy.spin()

