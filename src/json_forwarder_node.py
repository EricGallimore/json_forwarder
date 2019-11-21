#!/usr/bin/env python
from __future__ import unicode_literals

import rospy
from rospy.msg import AnyMsg
from std_msgs.msg import String
from functools import partial
from rospy_message_converter import json_message_converter
from importlib import import_module

class JsonForwarderNode(object):
    ''' This node will subscribe to the topics listed in the ~topic_list rosparam, convert them to json, and then
        publish them on /json/<topic_name>
    '''
    def __init__(self):

        rospy.init_node('json_forwarder')
        topic_list = rospy.get_param('~topic_list')

        self.publishers = {}
        for topic_name in topic_list:
            rospy.Subscriber(topic_name, AnyMsg, partial(self.on_incoming_message, topic_name=topic_name))
            pub_name = rospy.names.canonicalize_name('/json/' + topic_name)


            self.publishers[topic_name] = rospy.Publisher(pub_name, String, queue_size=10)


    def on_incoming_message(self, msg_data, topic_name):
        # type: (AnyMsg, str) -> None
        connection_header = msg_data._connection_header['type'].split('/')
        ros_pkg = connection_header[0] + '.msg'
        msg_type = connection_header[1]
        #print 'Message type detected as ' + msg_type
        msg_class = getattr(import_module(ros_pkg), msg_type)
        msg = msg_class()
        msg.deserialize(msg_data._buff)

        json_msg = json_message_converter.convert_ros_message_to_json(msg)
        self.publishers[topic_name].publish(json_msg)


if __name__ == '__main__':
    try:
        node = JsonForwarderNode()
        rospy.loginfo("JSON forwarder started")

        rate = rospy.Rate(1)
        while not rospy.is_shutdown():
            rate.sleep()

        rospy.loginfo("JSON forwarder shutdown")

    except rospy.ROSInterruptException:
        node.close()
        rospy.loginfo("JSON forwarder shutdown (interrupt)")