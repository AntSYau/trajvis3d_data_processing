import data_file
import protobuf.data_file_pb2 as pbdf
import protobuf.data_comm_pb2 as pbdc
import protobuf.data_comm_pb2_grpc as gdc
import grpc
from concurrent import futures
import pickle
import sys

test = pbdf.DataFile()

class TrajVis3DServicer(gdc.TrajVis3DServicer):
    dataFile = None

    def __init__(self, instructions):
        self.instructions = instructions
        print("ready to serve. timestamp between {} and {}. "
              "total number of valid timestamps: {}. "
              "total number of valid groups: {}".format(
            0,
            0,
            0,
            0
        ))

    def GetInstructionsBetween(self, request: pbdc.TimePeriod, context):
        print("request", request)
        for ts in range(request.start_ts, request.end_ts):
            if ts in self.instructions:
                _tmp = self.instructions[ts]
                print("{} serve {}".format(ts, len(_tmp)))
                for _inst in _tmp:
                    yield _inst


cached = True


def serve():
    fuck = data_file.DataFile(test.SerializeToString())
    fuck.group_data()
    fuck.generate_flow()
    fuck.clean_intermediates()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gdc.add_TrajVis3DServicer_to_server(TrajVis3DServicer(fuck.instructions), server)
    server.add_insecure_port('127.0.0.1:9623')
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("usage:\nserver.exe [file] [row#_uid] [row#_time] [row#_lat] [row#_lng]")
    else:
        test.file = sys.argv[1]
        test.sep = ","
        test.has_header = False
        test.row_uid = int(sys.argv[2])
        test.row_time = int(sys.argv[3])
        test.coord_sys = pbdf.CoordinateSystem.WGS84
        test.row_lat = int(sys.argv[4])
        test.row_lng = int(sys.argv[5])
        serve()
