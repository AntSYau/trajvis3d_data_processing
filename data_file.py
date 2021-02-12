import protobuf.data_file_pb2 as dfpb

class DataFile:
    def __init__(self, message):
        self.proto=dfpb.DataFile()
        self.proto.ParseFromString(message)