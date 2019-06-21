# ros imports
import esiaf_ros.msg as esiaf_msg


# Then some utilities classes

class AudioFormat:

    def __init__(self,
                 rate,
                 bitrate,
                 channels,
                 endian):
        try:
            self.rate = rate
            self.bitrate = bitrate
            self.channels = channels
            self.endian = endian
        except Exception as e:
            print('AudioFormat was created without proper parameters! ' + e.message)

    def to_ros(self):
        """
        Creates a representation of this AudioFormat in the form of a ros message object
        :return: a esiaf_ros.msg.AudioFormat object
        """
        rosified = esiaf_msg.AudioFormat()
        rosified.rate = self.rate
        rosified.bitrate = self.bitrate
        rosified.channels = self.channels
        rosified.endian = self.endian
        return rosified


class AudioTopicInfo:

    def __init__(self,
                 audioTopicInfo):
        self.topic = audioTopicInfo.topic
        self.allowedFormat = AudioFormat(audioTopicInfo.allowedFormat.rate,
                                           audioTopicInfo.allowedFormat.bitrate,
                                           audioTopicInfo.allowedFormat.channels,
                                           audioTopicInfo.allowedFormat.endian)

    def to_ros(self):
        rosified = esiaf_msg.AudioTopicInfo()
        rosified.topic = self.topic
        rosified.allowedFormat = self.allowedFormat.to_ros()
        return rosified

    def __str__(self):
        string = 'AudioTopicInfo[\n\t'
        string += 'topic: {}\n\t'.format(self.topic)
        string += 'allowedFormat: {}\n'.format(str(self.allowedFormat))
        string += ']'
        return string

